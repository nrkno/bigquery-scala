package no.nrk.bigquery

import no.nrk.bigquery.implicits._
import cats.effect.kernel.Outcome
import cats.effect.{IO, Resource}
import cats.syntax.parallel._
import cats.syntax.show._
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
import util.StreamUtils
import fs2.{Chunk, Stream}
import io.circe.Encoder
import org.apache.avro
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.threeten.bp.Duration
import org.typelevel.log4cats.slf4j._

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class BigQueryClient(
    bigQuery: BigQuery,
    val reader: BigQueryReadClient,
    val track: BQTracker
) {
  protected lazy val logger = Slf4jFactory.getLogger[IO]

  def underlying: BigQuery = bigQuery

  /** Synchronous query to BQ.
    *
    * Must be called with the type of the row. The type must have a [[BQRead]]
    * instance.
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
      legacySql: Boolean = false,
      jobOptions: Seq[JobOption] = Nil,
      logStream: Boolean = false
  ): Stream[IO, A] =
    Stream
      .resource(
        synchronousQueryExecute(
          jobName,
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
      jobName: BQJobName,
      query: BQSqlFrag,
      legacySql: Boolean = false,
      jobOptions: Seq[JobOption] = Nil,
      logStream: Boolean = false
  ): Resource[IO, (avro.Schema, Stream[IO, GenericRecord])] = {

    val runQuery: IO[Job] = {
      val queryRequest = QueryJobConfiguration
        .newBuilder(query.asStringWithUDFs)
        .setUseLegacySql(legacySql)
        .build
      submitJob(jobName)(jobId =>
        IO.blocking(
          Option(
            bigQuery.create(JobInfo.of(jobId, queryRequest), jobOptions: _*)
          )
        )
      ).flatMap {
        case Some(job) => IO.pure(job)
        case None =>
          IO.raiseError(
            new Exception(s"Unexpected: got no job after submitting $jobName")
          )
      }
    }

    def openServerStreams(
        job: Job,
        numStreams: Int
    ): Resource[IO, (ReadSession, List[ServerStream[ReadRowsResponse]])] = {
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
        session <- Resource.eval(IO.blocking(reader.createReadSession(request)))
        serverStreams <- 0.until(session.getStreamsCount).toList.parTraverse {
          streamN =>
            Resource.make(
              IO.blocking(
                reader.readRowsCallable.call(
                  ReadRowsRequest.newBuilder
                    .setReadStream(session.getStreams(streamN).getName)
                    .build
                )
              )
            )(serverStream => IO.blocking(serverStream.cancel()))
        }
      } yield (session, serverStreams)
    }

    def rows(
        datumReader: GenericDatumReader[GenericRecord],
        stream: ServerStream[ReadRowsResponse]
    ): Stream[IO, GenericRecord] =
      Stream
        .fromBlockingIterator[IO]
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
      val baseStream: Stream[IO, GenericRecord] = streams
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

  /** @return
    *   None, if `chunkedStream` is empty
    */
  def loadJson[A: Encoder, P: TableOps](
      jobName: BQJobName,
      table: BQTableDef.Table[P],
      partition: P,
      stream: fs2.Stream[IO, A],
      writeDisposition: WriteDisposition,
      chunkSize: Int = 10 * StreamUtils.Megabyte,
      logStream: Boolean = false
  ): IO[Option[LoadStatistics]] =
    submitJob(jobName) { jobId =>
      val partitionId = table.assertPartition(partition)
      val formatOptions = FormatOptions.json()
      val schema = table.schema

      val writeChannelConfiguration = WriteChannelConfiguration
        .newBuilder(partitionId.asTableId)
        .setWriteDisposition(writeDisposition)
        .setFormatOptions(formatOptions)
        .setSchema(schema.toSchema)
        .build()

      val writerResource: Resource[IO, TableDataWriteChannel] =
        Resource.make(
          IO.blocking(bigQuery.writer(jobId, writeChannelConfiguration))
        )(writer => IO.blocking(writer.close()))

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
            .evalMap(chunk => IO.blocking(writer.write(chunk.toByteBuffer)))
            .compile
            .drain
        }
        .flatMap(_ => IO.blocking(Option(bigQuery.getJob(jobId))))

    }.map(jobOpt => jobOpt.map(_.getStatistics[LoadStatistics]))

  def createTempTable[Param](
      table: BQTableDef.Table[Param],
      expirationDuration: Option[FiniteDuration] = Some(1.hour)
  ): IO[BQTableDef.Table[Param]] = {
    // a copy of `table` with new coordinates
    val tempTableDef = table.copy(
      TableId.of(
        "nrk-datahub",
        "tmp",
        table.tableId.getTable + UUID.randomUUID().toString
      )
    )
    val tempTableBqDef = UpdateOperation.createNew(tempTableDef).table
    val expirationTime =
      Instant.now.plusMillis(expirationDuration.getOrElse(1.hour).toMillis)

    val tempTableBqDefWithExpiry = tempTableBqDef.toBuilder
      .setExpirationTime(expirationTime.toEpochMilli)
      .build()

    IO.blocking(bigQuery.create(tempTableBqDefWithExpiry))
      .map(_ => tempTableDef)
  }

  /** Submit any SQL statement to BQ, perfect for BQ to BQ insertions or data
    * mutation
    */
  def submitQuery[P](
      jobName: BQJobName,
      query: BQSqlFrag,
      destination: Option[BQPartitionId[P]] = None,
      writeDisposition: Option[WriteDisposition] = None,
      timePartitioning: Option[TimePartitioning] = None,
      jobOptions: Seq[JobOption] = Nil
  ): IO[Job] =
    submitJob(jobName) { jobId =>
      val jobConfiguration = {
        val b = QueryJobConfiguration.newBuilder(query.asStringWithUDFs)
        destination.foreach(partitionId =>
          b.setDestinationTable(partitionId.asTableId)
        )
        writeDisposition.foreach(b.setWriteDisposition)
        timePartitioning.foreach(b.setTimePartitioning)
        b.build()
      }

      IO.blocking(
        Option(
          bigQuery.create(JobInfo.of(jobId, jobConfiguration), jobOptions: _*)
        )
      )
    }.flatMap {
      case Some(job) => IO.pure(job)
      case None =>
        IO.raiseError(
          new Exception(s"Unexpected: got no job after submitting $jobName")
        )
    }

  /** Submit a job to BQ, wait for it to finish, log results, track as
    * dependency
    */
  def submitJob(
      jobName: BQJobName
  )(runJob: JobId => IO[Option[Job]]): IO[Option[Job]] =
    IO(System.currentTimeMillis)
      .product(jobName.freshJobId.flatMap(runJob))
      .flatMap {
        case (t0, Some(runningJob)) =>
          val mkDuration = IO(System.currentTimeMillis)
            .map(t1 => FiniteDuration.apply(t1 - t0, TimeUnit.MILLISECONDS))

          val logged: IO[Job] =
            BQPoll
              .poll(
                runningJob,
                baseDelay = 3.second,
                maxDuration = 20.minutes,
                maxErrorsTolerated = 10
              )(
                retry = IO.blocking(bigQuery.getJob(runningJob.getJobId))
              )
              .flatMap {
                case BQPoll.Failed(error) => IO.raiseError(error)
                case BQPoll.Success(job)  => IO.pure(job)
              }
              .guaranteeCase {
                case Outcome.Errored(e) =>
                  for {
                    _ <- logger.warn(e)(show"${runningJob.show} failed")
                    duration <- mkDuration
                    _ <- track(
                      duration,
                      jobName,
                      isSuccess = false,
                      stats = None
                    )
                  } yield ()

                case Outcome.Canceled() =>
                  for {
                    _ <- logger.warn(show"${runningJob.show} cancelled")
                    duration <- mkDuration
                    _ <- track(
                      duration,
                      jobName,
                      isSuccess = false,
                      stats = None
                    )
                  } yield ()
                case Outcome.Succeeded(_) =>
                  IO.unit // we don't have access to the completed job here
              }
              .flatTap(succeededJob =>
                for {
                  _ <- logger.debug(show"${succeededJob.show} succeeded")
                  duration <- mkDuration
                  _ <- track(
                    duration,
                    jobName,
                    isSuccess = true,
                    stats = Option(succeededJob.getStatistics[JobStatistics])
                  )
                } yield ()
              )

          logged.map(Some.apply)

        case (_, None) =>
          IO.pure(None)
      }

  def getTable(
      tableId: TableId,
      tableOptions: Seq[TableOption] = Nil
  ): IO[Option[Table]] =
    IO.blocking(
      Option(bigQuery.getTable(tableId, tableOptions: _*)).filter(_.exists())
    )

  def tableExists(tableId: TableId): IO[Table] =
    getTable(tableId).flatMap {
      case None =>
        IO.raiseError(new RuntimeException(s"Table $tableId does not exists"))
      case Some(table) => IO.pure(table)
    }

  def dryRun(jobName: BQJobName, query: BQSqlFrag): IO[Job] =
    jobName.freshJobId.flatMap { jobId =>
      val jobInfo = JobInfo.of(
        jobId,
        QueryJobConfiguration
          .newBuilder(query.asStringWithUDFs)
          .setDryRun(true)
          .build()
      )
      IO.blocking(bigQuery.create(jobInfo))
    }

  def create(table: TableInfo): IO[Table] =
    IO(bigQuery.create(table))

  def update(table: TableInfo): IO[Table] =
    IO(bigQuery.update(table))

  def delete(tableId: TableId): IO[Boolean] =
    IO(bigQuery.delete(tableId))

  def tablesIn(
      datasetId: DatasetId,
      datasetOptions: Seq[BigQuery.TableListOption] = Nil
  ): IO[Vector[BQTableRef[Any]]] =
    IO.blocking(bigQuery.listTables(datasetId, datasetOptions: _*)).flatMap {
      tables =>
        tables
          .iterateAll()
          .asScala
          .toVector
          .parTraverseFilter { table =>
            val tableId = table.getTableId
            table.getDefinition[TableDefinition] match {
              case definition: StandardTableDefinition =>
                BQPartitionType.from(definition) match {
                  case Right(partitionType) =>
                    IO.pure(Some(BQTableRef(tableId, partitionType)))
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

  def readerResource(
      credentials: Credentials
  ): Resource[IO, BigQueryReadClient] =
    Resource.fromAutoCloseable(
      IO.blocking {
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

  def fromCredentials(
      credentials: Credentials,
      configure: Option[BigQueryOptions.Builder => BigQueryOptions.Builder] =
        None
  ): IO[BigQuery] =
    IO.blocking {
      val conf = configure.getOrElse(defaultConfigure)
      conf(BigQueryOptions.newBuilder())
        .setCredentials(credentials)
        .build()
        .getService
    }

  def resource(
      credentials: Credentials,
      tracker: BQTracker,
      configure: Option[BigQueryOptions.Builder => BigQueryOptions.Builder] =
        None
  ): Resource[IO, BigQueryClient] =
    for {
      bq <- Resource.eval(
        BigQueryClient.fromCredentials(credentials, configure)
      )
      bqRead <- BigQueryClient.readerResource(credentials)
    } yield new BigQueryClient(bq, bqRead, tracker)
}
