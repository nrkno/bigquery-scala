package no.nrk.bigquery

import cats.effect.implicits._
import cats.effect.kernel.Outcome
import cats.effect.{Async, Resource}
import cats.syntax.all._
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.api.gax.retrying.RetrySettings
import com.google.api.gax.rpc.ServerStream
import com.google.auth.Credentials
import com.google.cloud.bigquery.BigQuery.{JobOption, TableOption}
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.google.cloud.bigquery.JobStatistics.LoadStatistics
import com.google.cloud.bigquery.storage.v1._
import com.google.cloud.bigquery.{Option => _, _}
import com.google.cloud.http.HttpTransportOptions
import fs2.{Chunk, Stream}
import io.circe.Encoder
import no.nrk.bigquery.syntax.showJob
import no.nrk.bigquery.internal.TableUpdateOperation
import no.nrk.bigquery.metrics.{BQMetrics, MetricsOps}
import no.nrk.bigquery.util.StreamUtils
import org.apache.avro
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.threeten.bp.Duration
import org.typelevel.log4cats.LoggerFactory

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class BigQueryClient[F[_]](
    bigQuery: BigQuery,
    val reader: BigQueryReadClient,
    val metricOps: MetricsOps[F]
)(implicit F: Async[F], lf: LoggerFactory[F]) {
  private val logger = lf.getLogger

  def underlying: BigQuery = bigQuery

  def synchronousQuery[A](
      jobName: BQJobName,
      query: BQQuery[A]
  ): Stream[F, A] =
    synchronousQuery(jobName, query, legacySql = false)
  def synchronousQuery[A](
      jobName: BQJobName,
      query: BQQuery[A],
      legacySql: Boolean
  ): Stream[F, A] =
    synchronousQuery(jobName, query, legacySql, Nil)

  def synchronousQuery[A](
      jobName: BQJobName,
      query: BQQuery[A],
      legacySql: Boolean,
      jobOptions: Seq[JobOption]
  ): Stream[F, A] =
    synchronousQuery(jobName, query, legacySql, jobOptions, logStream = true)

  def synchronousQuery[A](
      jobName: BQJobName,
      query: BQQuery[A],
      legacySql: Boolean,
      jobOptions: Seq[JobOption],
      logStream: Boolean
  ): Stream[F, A] =
    synchronousQuery(jobName, query, legacySql, jobOptions, logStream, None)

  /** Synchronous query to BQ.
    *
    * Must be called with the type of the row. The type must have a [[BQRead]] instance.
    *
    * Example:
    * {{{
    * type RowType = (String, Date, String)
    * val row: Stream[IO, RowType] = client.synchronousQuery[RowType]("SELECT id, date, comment FROM tableName")
    * }}}
    */
  def synchronousQuery[A](
      jobName: BQJobName,
      query: BQQuery[A],
      legacySql: Boolean,
      jobOptions: Seq[JobOption],
      logStream: Boolean,
      locationId: Option[LocationId]
  ): Stream[F, A] =
    Stream
      .resource(
        synchronousQueryExecute(
          jobName,
          query.sql,
          legacySql,
          jobOptions,
          logStream,
          locationId
        )
      )
      .flatMap { case (_, rowStream) =>
        rowStream.map { (record: GenericRecord) =>
          record.getSchema.getFields.size() match {
            // this corresponds to the support for `AnyVal` in magnolia.
            case 1 =>
              query.bqRead.read(
                record.getSchema.getFields.get(0).schema(),
                record.get(0)
              )
            case _ => query.bqRead.read(record.getSchema, record)
          }
        }
      }

  protected def synchronousQueryExecute(
      jobName: BQJobName,
      query: BQSqlFrag,
      legacySql: Boolean,
      jobOptions: Seq[JobOption],
      logStream: Boolean,
      locationId: Option[LocationId]
  ): Resource[F, (avro.Schema, Stream[F, GenericRecord])] = {

    val runQuery: F[Job] = {
      val queryRequest = QueryJobConfiguration
        .newBuilder(query.asStringWithUDFs)
        .setUseLegacySql(legacySql)
        .build
      submitJob(jobName, locationId)(jobId =>
        F.blocking(
          Option(
            bigQuery.create(JobInfo.of(jobId, queryRequest), jobOptions: _*)
          )
        )).flatMap {
        case Some(job) => F.pure(job)
        case None =>
          F.raiseError(
            new Exception(s"Unexpected: got no job after submitting $jobName")
          )
      }
    }

    def openServerStreams(
        job: Job,
        numStreams: Int
    ): Resource[F, (ReadSession, List[ServerStream[ReadRowsResponse]])] = {
      val tempTable =
        job.getConfiguration[QueryJobConfiguration].getDestinationTable

      val request = CreateReadSessionRequest
        .newBuilder()
        .setReadSession(
          ReadSession
            .newBuilder()
            .setTable(
              s"projects/${tempTable.getProject}/datasets/${tempTable.getDataset}/tables/${tempTable.getTable}"
            )
            .setDataFormat(DataFormat.AVRO)
        )
        .setParent(s"projects/${tempTable.getProject}")
        .setMaxStreamCount(numStreams)
        .build()

      for {
        session <- Resource.eval(F.blocking(reader.createReadSession(request)))
        serverStreams <- 0.until(session.getStreamsCount).toList.parTraverse { streamN =>
          Resource.make(
            F.blocking(
              reader.readRowsCallable.call(
                ReadRowsRequest.newBuilder
                  .setReadStream(session.getStreams(streamN).getName)
                  .build
              )
            )
          )(serverStream => F.blocking(serverStream.cancel()))
        }
      } yield (session, serverStreams)
    }

    def rows(
        datumReader: GenericDatumReader[GenericRecord],
        stream: ServerStream[ReadRowsResponse]
    ): Stream[F, GenericRecord] =
      Stream
        .fromBlockingIterator[F]
        .apply(stream.iterator.asScala, chunkSize = 1)
        .flatMap { response =>
          val b = Vector.newBuilder[GenericRecord]
          val avroRows = response.getAvroRows.getSerializedBinaryRows
          val decoder =
            DecoderFactory.get.binaryDecoder(avroRows.toByteArray, null)

          while (!decoder.isEnd)
            b += datumReader.read(null, decoder)
          Stream.chunk(Chunk.vector(b.result()))
        }

    for {
      job <- Resource.liftK(runQuery)
      tuple <- openServerStreams(job, numStreams = 4)
      (session, streams) = tuple
      schema = new avro.Schema.Parser().parse(session.getAvroSchema.getSchema)
      datumReader = new GenericDatumReader[GenericRecord](schema)
    } yield {
      val baseStream: Stream[F, GenericRecord] = streams
        .map(stream => rows(datumReader, stream))
        .reduceOption(_.merge(_))
        .getOrElse(Stream.empty)

      val rowStream =
        if (logStream)
          baseStream.chunks
            .through(StreamUtils.logChunks(logger, None, "downloading"))
            .flatMap(Stream.chunk)
        else baseStream

      (schema, rowStream)
    }
  }

  def loadJson[A: Encoder, P: TableOps](
      jobName: BQJobName,
      table: BQTableDef.Table[P],
      partition: P,
      stream: fs2.Stream[F, A],
      writeDisposition: WriteDisposition
  ): F[Option[LoadStatistics]] = loadJson(
    jobName = jobName,
    table = table,
    partition = partition,
    stream = stream,
    writeDisposition = writeDisposition,
    logStream = false
  )

  def loadJson[A: Encoder, P: TableOps](
      jobName: BQJobName,
      table: BQTableDef.Table[P],
      partition: P,
      stream: fs2.Stream[F, A],
      writeDisposition: WriteDisposition,
      logStream: Boolean
  ): F[Option[LoadStatistics]] =
    loadJson(
      jobName = jobName,
      table = table,
      partition = partition,
      stream = stream,
      writeDisposition = writeDisposition,
      logStream = logStream,
      chunkSize = 10 * StreamUtils.Megabyte
    )

  /** @return
    *   None, if `chunkedStream` is empty
    */
  def loadJson[A: Encoder, P: TableOps](
      jobName: BQJobName,
      table: BQTableDef.Table[P],
      partition: P,
      stream: fs2.Stream[F, A],
      writeDisposition: WriteDisposition,
      logStream: Boolean,
      chunkSize: Int
  ): F[Option[LoadStatistics]] =
    submitJob(jobName, table.tableId.dataset.location) { jobId =>
      val partitionId = table.assertPartition(partition)
      val formatOptions = FormatOptions.json()
      val schema = table.schema

      val writeChannelConfiguration = WriteChannelConfiguration
        .newBuilder(partitionId.asTableId.underlying)
        .setWriteDisposition(writeDisposition)
        .setFormatOptions(formatOptions)
        .setSchema(schema.toSchema)
        .build()

      val writerResource: Resource[F, TableDataWriteChannel] =
        Resource.make(
          F.blocking(bigQuery.writer(jobId, writeChannelConfiguration))
        )(writer => F.blocking(writer.close()))

      writerResource
        .use { writer =>
          stream
            .through(StreamUtils.toLineSeparatedJsonBytes(chunkSize))
            .through(
              if (logStream)
                StreamUtils
                  .logChunks(logger, None, show"uploading to $partitionId")
              else identity
            )
            .prefetch
            .evalMap(chunk => F.interruptible(writer.write(chunk.toByteBuffer)))
            .compile
            .drain
        }
        .flatMap(_ => F.interruptible(Option(bigQuery.getJob(jobId))))

    }.map(jobOpt => jobOpt.map(_.getStatistics[LoadStatistics]))

  def createTempTable[Param](
      table: BQTableDef.Table[Param],
      tmpDataset: BQDataset
  ): F[BQTableDef.Table[Param]] =
    createTempTable(table, tmpDataset, Some(1.hour))

  def createTempTable[Param](
      table: BQTableDef.Table[Param],
      tmpDataset: BQDataset,
      expirationDuration: Option[FiniteDuration]
  ): F[BQTableDef.Table[Param]] = F
    .delay {
      // a copy of `table` with new coordinates
      table.copy(tableId = BQTableId(
        tmpDataset,
        table.tableId.tableName + UUID.randomUUID().toString
      ))
    }
    .flatMap(tmp =>
      F.interruptible {
        val tempTableBqDef = TableUpdateOperation.createNew(tmp).table
        val expirationTime =
          Instant.now.plusMillis(expirationDuration.getOrElse(1.hour).toMillis)

        val tempTableBqDefWithExpiry = tempTableBqDef.toBuilder
          .setExpirationTime(expirationTime.toEpochMilli)
          .build()

        bigQuery.create(tempTableBqDefWithExpiry)
      }.as(tmp))

  def createTempTableResource[Param](
      table: BQTableDef.Table[Param],
      tmpDataset: BQDataset): Resource[F, BQTableDef.Table[Param]] =
    Resource.make(createTempTable(table, tmpDataset))(tmp => delete(tmp.tableId).attempt.void)

  def submitQuery[P](jobName: BQJobName, query: BQSqlFrag): F[Job] =
    submitQuery(jobName, query, None)

  def submitQuery[P](
      jobName: BQJobName,
      query: BQSqlFrag,
      locationId: Option[LocationId]
  ): F[Job] = submitQuery(jobName, query, locationId, None)

  def submitQuery[P](
      jobName: BQJobName,
      query: BQSqlFrag,
      locationId: Option[LocationId],
      destination: Option[BQPartitionId[P]]
  ): F[Job] =
    submitQuery(jobName, query, locationId, destination, None)

  def submitQuery[P](
      jobName: BQJobName,
      query: BQSqlFrag,
      locationId: Option[LocationId],
      destination: Option[BQPartitionId[P]],
      writeDisposition: Option[WriteDisposition]
  ): F[Job] = submitQuery(
    jobName,
    query,
    locationId,
    destination,
    writeDisposition,
    None
  )

  def submitQuery[P](
      jobName: BQJobName,
      query: BQSqlFrag,
      locationId: Option[LocationId],
      destination: Option[BQPartitionId[P]],
      writeDisposition: Option[WriteDisposition],
      timePartitioning: Option[TimePartitioning]
  ): F[Job] = submitQuery(
    jobName,
    query,
    locationId,
    destination,
    writeDisposition,
    timePartitioning,
    Nil
  )

  /** Submit any SQL statement to BQ, perfect for BQ to BQ insertions or data mutation
    */
  def submitQuery[P](
      jobName: BQJobName,
      query: BQSqlFrag,
      locationId: Option[LocationId],
      destination: Option[BQPartitionId[P]],
      writeDisposition: Option[WriteDisposition],
      timePartitioning: Option[TimePartitioning],
      jobOptions: Seq[JobOption]
  ): F[Job] =
    submitJob(jobName, locationId) { jobId =>
      val jobConfiguration = {
        val b = QueryJobConfiguration.newBuilder(query.asStringWithUDFs)
        destination.foreach(partitionId => b.setDestinationTable(partitionId.asTableId.underlying))
        writeDisposition.foreach(b.setWriteDisposition)
        timePartitioning.foreach(b.setTimePartitioning)
        b.build()
      }

      F.interruptible(
        Option(
          bigQuery.create(JobInfo.of(jobId, jobConfiguration), jobOptions: _*)
        )
      )
    }.flatMap {
      case Some(job) => F.pure(job)
      case None =>
        F.raiseError(
          new Exception(s"Unexpected: got no job after submitting $jobName")
        )
    }

  /** Submit a job to BQ, wait for it to finish, log results, track as dependency
    */
  def submitJob(jobName: BQJobName, location: Option[LocationId])(
      runJob: JobId => F[Option[Job]]
  ): F[Option[Job]] = {
    val loggedJob: JobId => F[Option[Job]] = id =>
      runJob(id).flatMap {
        case Some(runningJob) =>
          val logged: F[Job] =
            BQPoll
              .poll[F](
                runningJob,
                baseDelay = 3.second,
                maxDuration = 20.minutes,
                maxErrorsTolerated = 10
              )(
                retry = F.interruptible(bigQuery.getJob(runningJob.getJobId))
              )
              .flatMap {
                case BQPoll.Failed(error) => F.raiseError[Job](error)
                case BQPoll.Success(job) => F.pure(job)
              }
              .guaranteeCase {
                case Outcome.Errored(e) =>
                  logger.warn(e)(show"${runningJob.show} failed")
                case Outcome.Canceled() =>
                  logger.warn(show"${runningJob.show} cancelled")
                case Outcome.Succeeded(_) =>
                  logger.debug(show"${runningJob.show} succeeded")
              }

          logged.map(Some.apply)

        case None =>
          F.pure(None)
      }

    jobName
      .freshJobId(location)
      .flatMap(id => BQMetrics(metricOps, jobName)(loggedJob(id)))
  }

  def getTable(
      tableId: BQTableId,
      tableOptions: TableOption*
  ): F[Option[Table]] =
    F.interruptible(
      Option(bigQuery.getTable(tableId.underlying, tableOptions: _*))
        .filter(_.exists())
    )

  def tableExists(tableId: BQTableId): F[Table] =
    getTable(tableId).flatMap {
      case None =>
        F.raiseError(new RuntimeException(s"Table $tableId does not exists"))
      case Some(table) => F.pure(table)
    }

  def dryRun(
      jobName: BQJobName,
      query: BQSqlFrag,
      location: Option[LocationId]
  ): F[Job] =
    jobName.freshJobId(location).flatMap { jobId =>
      val jobInfo = JobInfo.of(
        jobId,
        QueryJobConfiguration
          .newBuilder(query.asStringWithUDFs)
          .setDryRun(true)
          .build()
      )
      F.interruptible(bigQuery.create(jobInfo))
    }

  def create(table: TableInfo): F[Table] =
    F.blocking(bigQuery.create(table))

  def update(table: TableInfo): F[Table] =
    F.blocking(bigQuery.update(table))

  def delete(tableId: BQTableId): F[Boolean] =
    F.blocking(bigQuery.delete(tableId.underlying))

  def tablesIn(
      dataset: BQDataset,
      datasetOptions: BigQuery.TableListOption*
  ): F[Vector[BQTableRef[Any]]] =
    F.interruptible(bigQuery.listTables(dataset.underlying, datasetOptions: _*)).flatMap { tables =>
      tables
        .iterateAll()
        .asScala
        .toVector
        .parTraverseFilter { table =>
          val tableId = BQTableId.unsafeFromGoogle(dataset, table.getTableId)
          table.getDefinition[TableDefinition] match {
            case definition: StandardTableDefinition =>
              BQPartitionType.from(definition) match {
                case Right(partitionType) =>
                  F.pure(Some(BQTableRef(tableId, partitionType)))
                case Left(msg) =>
                  logger
                    .warn(
                      show"Ignoring $tableId because couldn't understand partitioning: $msg"
                    )
                    .as(None)
              }
            case notTable =>
              logger
                .warn(
                  show"Ignoring $tableId because we only consider tables, not ${notTable.getType.name}"
                )
                .as(None)
          }
        }
    }

  def getRoutine(udfId: UDF.UDFId.PersistentId): F[Option[Routine]] =
    F.interruptible {
      val routineId = RoutineId.of(udfId.dataset.project.value, udfId.dataset.id, udfId.name.value)
      Option(bigQuery.getRoutine(routineId)).filter(_.exists())
    }

  def create(info: RoutineInfo): F[Routine] =
    F.blocking(bigQuery.create(info))

  def update(info: RoutineInfo): F[Routine] =
    F.blocking(bigQuery.update(info))

  def delete(udfId: UDF.UDFId.PersistentId): F[Boolean] =
    F.blocking(bigQuery.delete(RoutineId.of(udfId.dataset.project.value, udfId.dataset.id, udfId.name.value)))

}

object BigQueryClient {
  val readTimeoutSecs = 20L
  val connectTimeoutSecs = 60L

  def defaultConfigure(
      builder: BigQueryOptions.Builder
  ): BigQueryOptions.Builder =
    builder
      .setTransportOptions(
        HttpTransportOptions
          .newBuilder()
          .setConnectTimeout(
            TimeUnit.SECONDS.toMillis(connectTimeoutSecs).toInt
          )
          .setReadTimeout(TimeUnit.SECONDS.toMillis(readTimeoutSecs).toInt)
          .build()
      )
      .setRetrySettings(
        RetrySettings
          .newBuilder()
          .setMaxAttempts(3)
          .setInitialRetryDelay(Duration.ofSeconds(1))
          .setMaxRetryDelay(Duration.ofMinutes(2))
          .setRetryDelayMultiplier(2.0)
          .setTotalTimeout(Duration.ofMinutes(5))
          .setInitialRpcTimeout(Duration.ZERO)
          .setRpcTimeoutMultiplier(1.0)
          .setMaxRpcTimeout(Duration.ZERO)
          .build()
      )

  def readerResource[F[_]](
      credentials: Credentials
  )(implicit F: Async[F]): Resource[F, BigQueryReadClient] =
    Resource.fromAutoCloseable(
      F.blocking {
        BigQueryReadClient.create(
          BigQueryReadSettings
            .newBuilder()
            .setCredentialsProvider(
              FixedCredentialsProvider.create(credentials)
            )
            .build()
        )
      }
    )

  def fromCredentials[F[_]](
      credentials: Credentials,
      configure: Option[BigQueryOptions.Builder => BigQueryOptions.Builder] = None
  )(implicit F: Async[F]): F[BigQuery] =
    F.blocking {
      val conf = configure.getOrElse(defaultConfigure(_))
      conf(BigQueryOptions.newBuilder())
        .setCredentials(credentials)
        .build()
        .getService
    }

  def resource[F[_]: Async: LoggerFactory](
      credentials: Credentials,
      metricsOps: MetricsOps[F],
      configure: Option[BigQueryOptions.Builder => BigQueryOptions.Builder] = None
  ): Resource[F, BigQueryClient[F]] =
    for {
      bq <- Resource.eval(
        BigQueryClient.fromCredentials(credentials, configure)
      )
      bqRead <- BigQueryClient.readerResource(credentials)
    } yield new BigQueryClient(bq, bqRead, metricsOps)
}
