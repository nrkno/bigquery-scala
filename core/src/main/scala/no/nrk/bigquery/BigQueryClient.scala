/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.Show
import cats.data.OptionT
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
import no.nrk.bigquery.internal.{PartitionTypeHelper, SchemaHelper, TableUpdateOperation}
import no.nrk.bigquery.internal.GoogleTypeHelper._
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
    val metricOps: MetricsOps[F],
    config: BigQueryClient.Config
)(implicit F: Async[F], lf: LoggerFactory[F]) {
  private val logger = lf.getLogger
  private implicit def showJob[J <: JobInfo]: Show[J] = Show.show(Jsonify.job)

  def underlying: BigQuery = bigQuery

  def synchronousQuery[A](
      jobId: BQJobId,
      query: BQQuery[A]
  ): Stream[F, A] =
    synchronousQuery(jobId, query, legacySql = false)

  def synchronousQuery[A](
      jobId: BQJobId,
      query: BQQuery[A],
      legacySql: Boolean
  ): Stream[F, A] =
    synchronousQuery(jobId, query, legacySql, Nil)

  def synchronousQuery[A](
      jobId: BQJobId,
      query: BQQuery[A],
      legacySql: Boolean,
      jobOptions: Seq[JobOption]
  ): Stream[F, A] =
    synchronousQuery(jobId, query, legacySql, jobOptions, logStream = true)

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
      jobId: BQJobId,
      query: BQQuery[A],
      legacySql: Boolean,
      jobOptions: Seq[JobOption],
      logStream: Boolean
  ): Stream[F, A] =
    Stream
      .resource(
        synchronousQueryExecute(
          jobId,
          query.sql,
          legacySql,
          jobOptions,
          logStream
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
      jobId: BQJobId,
      query: BQSqlFrag,
      legacySql: Boolean,
      jobOptions: Seq[JobOption],
      logStream: Boolean
  ): Resource[F, (avro.Schema, Stream[F, GenericRecord])] = {

    val runQuery: F[Job] = {
      val queryRequest = QueryJobConfiguration
        .newBuilder(query.asStringWithUDFs)
        .setUseLegacySql(legacySql)
        .setLabels(jobId.labels.value.asJava)
        .build
      submitJob(jobId)(jobId =>
        F.blocking(
          Option(
            bigQuery.create(JobInfo.of(jobId, queryRequest), jobOptions: _*)
          )
        )).flatMap {
        case Some(job) => F.pure(job)
        case None =>
          F.raiseError(
            new Exception(s"Unexpected: got no job after submitting ${jobId.name}")
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
          Stream.chunk(Chunk.from(b.result()))
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
      jobId: BQJobId,
      table: BQTableDef.Table[P],
      partition: P,
      stream: fs2.Stream[F, A],
      writeDisposition: WriteDisposition
  ): F[Option[LoadStatistics]] = loadJson(
    jobId = jobId,
    table = table,
    partition = partition,
    stream = stream,
    writeDisposition = writeDisposition,
    logStream = false
  )

  def loadJson[A: Encoder, P: TableOps](
      jobId: BQJobId,
      table: BQTableDef.Table[P],
      partition: P,
      stream: fs2.Stream[F, A],
      writeDisposition: WriteDisposition,
      logStream: Boolean
  ): F[Option[LoadStatistics]] =
    loadJson(
      jobId = jobId,
      table = table,
      partition = partition,
      stream = stream,
      writeDisposition = writeDisposition,
      logStream = logStream,
      chunkSize = 10 * StreamUtils.Megabyte
    )

  def loadToHashedPartition[A](
      jobId: BQJobId,
      table: BQTableDef.Table[Long],
      stream: fs2.Stream[F, A]
  )(implicit hashedEncoder: HashedPartitionEncoder[A]): F[Option[LoadStatistics]] =
    loadToHashedPartition(jobId, table, stream, logStream = false)

  def loadToHashedPartition[A](
      jobId: BQJobId,
      table: BQTableDef.Table[Long],
      stream: fs2.Stream[F, A],
      logStream: Boolean
  )(implicit hashedEncoder: HashedPartitionEncoder[A]): F[Option[LoadStatistics]] =
    loadToHashedPartition(jobId, table, stream, logStream, chunkSize = 10 * StreamUtils.Megabyte)

  def loadToHashedPartition[A](
      jobId: BQJobId,
      table: BQTableDef.Table[Long],
      stream: fs2.Stream[F, A],
      logStream: Boolean,
      chunkSize: Int
  )(implicit hashedEncoder: HashedPartitionEncoder[A]): F[Option[LoadStatistics]] = {
    val partitionType = table.partitionType match {
      case x: BQPartitionType.IntegerRangePartitioned => x
    }

    loadJson(
      jobId,
      table.tableId,
      table.schema,
      stream.map(x => hashedEncoder.toJson(x, partitionType)),
      WriteDisposition.WRITE_APPEND,
      logStream,
      chunkSize
    )
  }

  /** @return
    *   None, if `chunkedStream` is empty
    */
  def loadJson[A: Encoder, P: TableOps](
      jobId: BQJobId,
      table: BQTableDef.Table[P],
      partition: P,
      stream: fs2.Stream[F, A],
      writeDisposition: WriteDisposition,
      logStream: Boolean,
      chunkSize: Int
  ): F[Option[LoadStatistics]] = {
    val tableId = table.assertPartition(partition).asTableId
    loadJson(jobId, tableId, table.schema, stream, writeDisposition, logStream, chunkSize)
  }

  /** @return
    *   None, if `chunkedStream` is empty
    */
  private def loadJson[A: Encoder](
      id: BQJobId,
      tableId: BQTableId,
      schema: BQSchema,
      stream: fs2.Stream[F, A],
      writeDisposition: WriteDisposition,
      logStream: Boolean,
      chunkSize: Int
  ): F[Option[LoadStatistics]] =
    submitJob(id) { jobId =>
      val formatOptions = FormatOptions.json()

      val writeChannelConfiguration = WriteChannelConfiguration
        .newBuilder(tableId.underlying)
        .setWriteDisposition(writeDisposition)
        .setFormatOptions(formatOptions)
        .setSchema(SchemaHelper.toSchema(schema))
        .setLabels(id.labels.value.asJava)
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
                  .logChunks(logger, None, show"uploading to $tableId")
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
      tmpDataset: BQDataset.Ref
  ): F[BQTableDef.Table[Param]] =
    createTempTable(table, tmpDataset, Some(1.hour))

  def createTempTable[Param](
      table: BQTableDef.Table[Param],
      tmpDataset: BQDataset.Ref,
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
      tmpDataset: BQDataset.Ref): Resource[F, BQTableDef.Table[Param]] =
    Resource.make(createTempTable(table, tmpDataset))(tmp => delete(tmp.tableId).attempt.void)

  def submitQuery[P](jobId: BQJobId, query: BQSqlFrag): F[Job] =
    submitQuery(jobId, query, None)

  def submitQuery[P](
      id: BQJobId,
      query: BQSqlFrag,
      destination: Option[BQPartitionId[P]]
  ): F[Job] =
    submitQuery(id, query, destination, None)

  def submitQuery[P](
      id: BQJobId,
      query: BQSqlFrag,
      destination: Option[BQPartitionId[P]],
      writeDisposition: Option[WriteDisposition]
  ): F[Job] = submitQuery(
    id,
    query,
    destination,
    writeDisposition,
    None
  )

  def submitQuery[P](
      id: BQJobId,
      query: BQSqlFrag,
      destination: Option[BQPartitionId[P]],
      writeDisposition: Option[WriteDisposition],
      timePartitioning: Option[TimePartitioning]
  ): F[Job] = submitQuery(
    id,
    query,
    destination,
    writeDisposition,
    timePartitioning,
    Nil
  )

  /** Submit any SQL statement to BQ, perfect for BQ to BQ insertions or data mutation
    */
  def submitQuery[P](
      id: BQJobId,
      query: BQSqlFrag,
      destination: Option[BQPartitionId[P]],
      writeDisposition: Option[WriteDisposition],
      timePartitioning: Option[TimePartitioning],
      jobOptions: Seq[JobOption]
  ): F[Job] =
    submitJob(id) { jobId =>
      val jobConfiguration = {
        val b = QueryJobConfiguration.newBuilder(query.asStringWithUDFs)
        destination.foreach(partitionId => b.setDestinationTable(partitionId.asTableId.underlying))
        writeDisposition.foreach(b.setWriteDisposition)
        timePartitioning.foreach(b.setTimePartitioning)
        b.setLabels(id.labels.value.asJava)
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
          new Exception(s"Unexpected: got no job after submitting ${id.name}")
        )
    }

  /** Submit a job to BQ, wait for it to finish, log results, track as dependency
    */
  def submitJob(jobId: BQJobId)(
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
                maxDuration = config.jobTimeout,
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

    freshJobId(jobId)
      .flatMap(id => BQMetrics(metricOps, jobId)(loggedJob(id)))
  }

  def getTable(
      tableId: BQTableId,
      tableOptions: TableOption*
  ): F[Option[Table]] =
    F.interruptible(
      Option(bigQuery.getTable(tableId.underlying, tableOptions: _*))
        .filter(_.exists())
    )

  def getTableLike(tableId: BQTableId, tableOptions: TableOption*): F[Option[BQTableDef[Any]]] =
    OptionT(getTable(tableId, tableOptions: _*)).mapFilter(t => SchemaHelper.fromTable(t).toOption).value

  def tableExists(tableId: BQTableId): F[Table] =
    getTable(tableId).flatMap {
      case None =>
        F.raiseError(new RuntimeException(s"Table $tableId does not exists"))
      case Some(table) => F.pure(table)
    }

  def tableLikeExists(tableId: BQTableId): F[BQTableDef[Any]] =
    getTableLike(tableId).flatMap {
      case None =>
        F.raiseError(new RuntimeException(s"Table $tableId does not exists"))
      case Some(table) => F.pure(table)
    }

  def dryRun(
      id: BQJobId,
      query: BQSqlFrag
  ): F[Job] =
    freshJobId(id).flatMap { jobId =>
      val jobInfo = JobInfo.of(
        jobId,
        QueryJobConfiguration
          .newBuilder(query.asStringWithUDFs)
          .setDryRun(true)
          .setLabels(id.labels.value.asJava)
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

  def datasetsInProject(project: ProjectId): F[Vector[BQDataset]] =
    F.interruptible(
      bigQuery.listDatasets(project.value).iterateAll().asScala.toVector
    ).map(_.mapFilter(ds =>
      BQDataset.of(project, ds.getDatasetId.getDataset, Option(ds.getLocation).map(s => LocationId(s))).toOption))

  def tablesIn(
      dataset: BQDataset.Ref,
      datasetOptions: BigQuery.TableListOption*
  ): F[Vector[BQTableRef[Any]]] =
    F.interruptible(bigQuery.listTables(dataset.underlying, datasetOptions: _*)).flatMap { tables =>
      tables
        .iterateAll()
        .asScala
        .toVector
        .parTraverseFilter { table =>
          val tableId = unsafeTableIdFromGoogle(dataset, table.getTableId)
          table.getDefinition[TableDefinition] match {
            case definition: StandardTableDefinition =>
              PartitionTypeHelper.from(definition) match {
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

  def getRoutine(persistentId: BQPersistentRoutine.Id): F[Option[Routine]] =
    F.interruptible {
      val routineId = RoutineId.of(persistentId.dataset.project.value, persistentId.dataset.id, persistentId.name.value)
      Option(bigQuery.getRoutine(routineId)).filter(_.exists())
    }

  def create(info: RoutineInfo): F[Routine] =
    F.blocking(bigQuery.create(info))

  def update(info: RoutineInfo): F[Routine] =
    F.blocking(bigQuery.update(info))

  def delete(udfId: UDF.UDFId.PersistentId): F[Boolean] =
    F.blocking(bigQuery.delete(RoutineId.of(udfId.dataset.project.value, udfId.dataset.id, udfId.name.value)))

  private def freshJobId(id: BQJobId): F[JobId] = {
    val withDefaults = id.withDefaults(config.defaults)

    F.delay(
      JobId
        .newBuilder()
        .setJob(s"${withDefaults.name}-${UUID.randomUUID}")
        .setLocation(withDefaults.locationId.map(_.value).orNull)
        .setProject(withDefaults.projectId.map(_.value).orNull)
        .build()
    )
  }
}

object BigQueryClient {
  val readTimeoutSecs = 20L
  val connectTimeoutSecs = 60L

  case class Config(
      jobTimeout: FiniteDuration,
      defaults: Option[BQClientDefaults]
  )

  object Config {
    val default: Config =
      Config(
        jobTimeout = new FiniteDuration(20, TimeUnit.MINUTES),
        defaults = None
      )
  }

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
      configure: Option[BigQueryOptions.Builder => BigQueryOptions.Builder]
  )(implicit F: Async[F]): F[BigQuery] =
    fromCredentials(credentials, configure, None)

  def fromCredentials[F[_]](
      credentials: Credentials,
      configure: Option[BigQueryOptions.Builder => BigQueryOptions.Builder] = None,
      clientDefaults: Option[BQClientDefaults] = None
  )(implicit F: Async[F]): F[BigQuery] =
    F.blocking {
      val conf = configure.getOrElse(defaultConfigure(_))
      val configured = conf(BigQueryOptions.newBuilder())
        .setCredentials(credentials)
      // overrides projectId from credentials if set.
      clientDefaults.foreach(defaults =>
        configured
          .setLocation(defaults.locationId.value)
          .setProjectId(defaults.projectId.value))
      configured
        .build()
        .getService
    }

  def resource[F[_]: Async: LoggerFactory](
      credentials: Credentials,
      metricsOps: MetricsOps[F],
      configure: Option[BigQueryOptions.Builder => BigQueryOptions.Builder] = None,
      clientConfig: Option[BigQueryClient.Config] = None
  ): Resource[F, BigQueryClient[F]] =
    for {
      bq <- Resource.eval(
        BigQueryClient.fromCredentials(credentials, configure, clientConfig.flatMap(_.defaults))
      )
      bqRead <- BigQueryClient.readerResource(credentials)
    } yield new BigQueryClient(bq, bqRead, metricsOps, clientConfig.getOrElse(BigQueryClient.Config.default))
}
