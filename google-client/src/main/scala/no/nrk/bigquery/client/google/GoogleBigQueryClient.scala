/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package client.google

import cats.Show
import cats.data.OptionT
import cats.effect.implicits.*
import cats.effect.kernel.Outcome
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.api.gax.retrying.RetrySettings
import com.google.api.gax.rpc.ServerStream
import com.google.auth.Credentials
import com.google.cloud.bigquery.JobInfo.WriteDisposition as GoogleWriteDisposition
import com.google.cloud.bigquery.JobStatistics.QueryStatistics
import com.google.cloud.bigquery.storage.v1.*
import com.google.cloud.bigquery.{Job as GoogleJob, Option as _, *}
import com.google.cloud.http.HttpTransportOptions
import fs2.{Chunk, Stream}
import io.circe.Encoder
import no.nrk.bigquery.client.google.internal.GoogleTypeHelper.*
import no.nrk.bigquery.client.google.internal.{PartitionTypeHelper, RoutineHelper, SchemaHelper, TableHelper}
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
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class GoogleBigQueryClient[F[_]](
    bigQuery: BigQuery,
    val reader: BigQueryReadClient,
    val metricOps: MetricsOps[F],
    config: GoogleBigQueryClient.Config
)(implicit F: Async[F], lf: LoggerFactory[F])
    extends QueryClient[F]
    with BQAdminClientWithUnderlying[F, RoutineInfo, TableInfo] {
  type LoadStatistics = com.google.cloud.bigquery.JobStatistics.LoadStatistics
  type Job = GoogleJob

  import no.nrk.bigquery.client.google.internal.GoogleBQPollImpl.instance

  private val logger = lf.getLogger
  private implicit def showJob[J <: JobInfo]: Show[J] = Show.show(Jsonify.job)
  private val poller = new BQPoll.Poller[F](config.poll)

  def underlying: BigQuery = bigQuery

  protected[bigquery] def synchronousQueryExecute(
      jobId: BQJobId,
      query: BQSqlFrag,
      legacySql: Boolean,
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
            bigQuery.create(JobInfo.of(jobId, queryRequest))
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
        .setWriteDisposition(toGoogleDisposition(writeDisposition))
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

  override def createTempTable[Param](
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
        val tempTableBqDef = TableHelper.createNew(tmp)
        val expirationTime =
          Instant.now.plusMillis(expirationDuration.getOrElse(1.hour).toMillis)

        val tempTableBqDefWithExpiry = tempTableBqDef.toBuilder
          .setExpirationTime(expirationTime.toEpochMilli)
          .build()

        bigQuery.create(tempTableBqDefWithExpiry)
      }.as(tmp))

  override def createTempTableResource[Param](
      table: BQTableDef.Table[Param],
      tmpDataset: BQDataset.Ref): Resource[F, BQTableDef.Table[Param]] =
    Resource.make(createTempTable(table, tmpDataset))(tmp => deleteTable(tmp.tableId).attempt.void)

  def submitQuery[P](
      id: BQJobId,
      query: BQSqlFrag,
      destination: Option[BQPartitionId[P]],
      writeDisposition: Option[WriteDisposition]
  ): F[Job] = submitJob(id) { jobId =>
    val jobConfiguration = {
      val b = QueryJobConfiguration.newBuilder(query.asStringWithUDFs)
      destination.foreach(partitionId => b.setDestinationTable(partitionId.asTableId.underlying))
      writeDisposition.map(toGoogleDisposition).foreach(b.setWriteDisposition)
      b.setLabels(id.labels.value.asJava)
      b.build()
    }

    F.interruptible(
      Option(
        bigQuery.create(JobInfo.of(jobId, jobConfiguration))
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
            poller
              .poll[Job](
                runningJob = runningJob,
                retry = F.interruptible(bigQuery.getJob(runningJob.getJobId).some)
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

  override def getTable(tableId: BQTableId): F[Option[BQTableDef[Any]]] =
    getTableWithUnderlying(tableId).map(_.map(_.our))

  override def createTable(table: BQTableDef[Any]): F[BQTableDef[Any]] =
    F.delay(TableHelper.toGoogle(table, None)).flatMap(info => runWithClient(_.create(info))).as(table)

  override def updateTable(table: BQTableDef[Any]): F[BQTableDef[Any]] =
    F.delay(TableHelper.toGoogle(table, None)).flatMap(info => runWithClient(_.update(info))).as(table)

  override def deleteTable(tableId: BQTableId): F[Boolean] =
    runWithClient(_.delete(tableId.underlying))

  private def getTableImpl(tableId: BQTableId): F[Option[TableInfo]] =
    runWithClient(bq => Option(bq.getTable(tableId.underlying)).filter(_.exists()))

  override private[bigquery] def getTableWithUnderlying(tableId: BQTableId): F[Option[ExistingTable[TableInfo]]] =
    OptionT(getTableImpl(tableId))
      .mapFilter(t => TableHelper.fromGoogle(t).toOption.map(tbl => ExistingTable(tbl, t)))
      .value

  override private[bigquery] def updateTableWithExisting(
      existing: ExistingTable[TableInfo],
      table: BQTableDef[Any]): F[BQTableDef[Any]] =
    F.delay(TableHelper.toGoogle(table, Some(existing.table)))
      .flatMap(info => runWithClient(_.update(info)))
      .as(table)

  def dryRun(
      id: BQJobId,
      query: BQSqlFrag
  ): F[(BQSchema, Job)] =
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
        .map(job => SchemaHelper.fromSchema(job.getStatistics[QueryStatistics].getSchema) -> job)
    }

  def datasetsInProject(project: ProjectId): F[Vector[BQDataset]] =
    F.interruptible(
      bigQuery.listDatasets(project.value).iterateAll().asScala.toVector
    ).map(_.mapFilter(ds =>
      BQDataset.of(project, ds.getDatasetId.getDataset, Option(ds.getLocation).map(s => LocationId(s))).toOption))

  def tablesInDataset(
      dataset: BQDataset.Ref
  ): F[Vector[BQTableRef[Any]]] =
    F.interruptible(bigQuery.listTables(dataset.underlying)).flatMap { tables =>
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

  def getRoutineImpl(persistentId: BQPersistentRoutine.Id): F[Option[RoutineInfo]] =
    runWithClient { bq =>
      val routineId = RoutineId.of(persistentId.dataset.project.value, persistentId.dataset.id, persistentId.name.value)
      Option(bq.getRoutine(routineId)).filter(_.exists())
    }

  override def getRoutine(id: BQPersistentRoutine.Id): F[Option[BQPersistentRoutine.Unknown]] =
    getRoutineWithUnderlying(id).map(_.map(_.our))

  override private[bigquery] def getRoutineWithUnderlying(
      id: BQPersistentRoutine.Id): F[Option[ExistingRoutine[RoutineInfo]]] =
    OptionT(getRoutineImpl(id)).map(r => ExistingRoutine(RoutineHelper.fromGoogle(r), r)).value

  override private[bigquery] def updateRoutineWithExisting(
      existing: ExistingRoutine[RoutineInfo],
      routine: BQPersistentRoutine.Unknown): F[BQPersistentRoutine.Unknown] =
    F.delay(RoutineHelper.toGoogle(routine, Some(existing.routine)))
      .flatMap(info => runWithClient(_.update(info)))
      .as(routine)

  override def createRoutine(routine: BQPersistentRoutine.Unknown): F[BQPersistentRoutine.Unknown] =
    F.delay(RoutineHelper.toGoogle(routine, None)).flatMap(info => runWithClient(_.create(info))).as(routine)

  override def updateRoutine(routine: BQPersistentRoutine.Unknown): F[BQPersistentRoutine.Unknown] =
    F.delay(RoutineHelper.toGoogle(routine, None)).flatMap(info => runWithClient(_.update(info))).as(routine)

  override def routinesInDataset(dataset: BQDataset.Ref): F[Vector[BQPersistentRoutine.Unknown]] =
    runWithClient(_.listRoutines(dataset.underlying)).flatMap(page =>
      F.delay(
        page
          .iterateAll()
          .asScala
          .toVector)
        .flatMap(_.parTraverseFilter(routine =>
          if (Set(RoutineHelper.UdfRoutineType, RoutineHelper.TvfRoutineType).contains(routine.getRoutineType)) {
            F.delay(RoutineHelper.fromGoogle(routine).some)
          } else
            logger
              .warn(
                show"Ignoring ${routine.getRoutineId} because we only consider UDF, TDF, not ${routine.getRoutineType}"
              )
              .as(None))))

  override def createDataset(dataset: BQDataset): F[BQDataset] = {
    val converted = {
      val b = DatasetInfo.newBuilder(dataset.underlying)
      dataset.location.foreach(loc => b.setLocation(loc.value))
      b.build()
    }
    runWithClient(_.create(converted)).as(dataset)
  }

  override def deleteDataset(dataset: BQDataset.Ref): F[Boolean] =
    runWithClient(_.delete(dataset.underlying))

  override def getDataset(dataset: BQDataset.Ref): F[Option[BQDataset]] =
    OptionT(getDatasetImpl(dataset.underlying))
      .map(ds =>
        BQDataset(
          ProjectId.unsafeFromString(ds.getDatasetId.getProject),
          ds.getDatasetId.getDataset,
          Option(ds.getLocation).map(LocationId.apply)))
      .value

  private def getDatasetImpl(id: DatasetId) =
    runWithClient(bq => Option(bq.getDataset(id)).filter(_.exists()))

  def deleteRoutine(udfId: BQPersistentRoutine.Id): F[Boolean] =
    runWithClient(_.delete(RoutineId.of(udfId.dataset.project.value, udfId.dataset.id, udfId.name.value)))

  private def runWithClient[A](f: BigQuery => A): F[A] = F.interruptible(f(bigQuery))

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

  private def toGoogleDisposition(writeDisposition: WriteDisposition) = writeDisposition match {
    case WriteDisposition.WRITE_TRUNCATE => GoogleWriteDisposition.WRITE_TRUNCATE
    case WriteDisposition.WRITE_APPEND => GoogleWriteDisposition.WRITE_APPEND
    case WriteDisposition.WRITE_EMPTY => GoogleWriteDisposition.WRITE_EMPTY
  }

  private implicit val showRoutineId: Show[RoutineId] =
    Show.show(r => s"`${r.getProject}.${r.getDataset}.${r.getRoutine}`")
}

object GoogleBigQueryClient {
  val readTimeoutSecs = 20L
  val connectTimeoutSecs = 60L

  case class Config(
      poll: QueryClient.PollConfig,
      defaults: Option[BQClientDefaults]
  )

  object Config {
    val default: Config =
      Config(
        poll = QueryClient.PollConfig(),
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
      clientConfig: Option[GoogleBigQueryClient.Config] = None
  ): Resource[F, GoogleBigQueryClient[F]] =
    for {
      bq <- Resource.eval(
        GoogleBigQueryClient.fromCredentials(credentials, configure, clientConfig.flatMap(_.defaults))
      )
      bqRead <- GoogleBigQueryClient.readerResource(credentials)
    } yield new GoogleBigQueryClient(
      bq,
      bqRead,
      metricsOps,
      clientConfig.getOrElse(GoogleBigQueryClient.Config.default))
}
