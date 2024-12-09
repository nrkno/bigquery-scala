/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.client.http4s

import cats.data.OptionT
import cats.effect.*
import cats.effect.implicits.*
import cats.syntax.all.*
import com.google.cloud.bigquery.storage.v1.storage.{
  BigQueryRead,
  CreateReadSessionRequest,
  ReadRowsRequest,
  ReadRowsResponse
}
import com.google.cloud.bigquery.storage.v1.stream.{DataFormat, ReadSession}
import fs2.{Chunk, Pipe, Stream}
import googleapis.bigquery.*
import io.circe.*
import io.circe.syntax.EncoderOps
import no.nrk.bigquery.*
import no.nrk.bigquery.client.http4s.internal.{JobHelper, SchemaHelper, TableHelper}
import no.nrk.bigquery.util.StreamUtils
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.http4s.*
import org.http4s.client.Client
import org.http4s.headers.`Content-Type`
import org.http4s.syntax.literals.*
import org.typelevel.ci.CIStringSyntax
import org.typelevel.log4cats.LoggerFactory

import java.util.UUID
import scala.concurrent.duration.*

class Http4sQueryClient[F[_]] private (
    client: Client[F],
    defaults: BQClientDefaults,
    pollConfig: QueryClient.PollConfig)(implicit F: Async[F], lf: LoggerFactory[F])
    extends QueryClient[F] {
  import no.nrk.bigquery.client.http4s.internal.Http4sBQPollImpl.*
  private val logger = lf.getLogger
  private val jobsClient = new JobsClient(client)
  private val tableClient = new TablesClient[F](client)
  private val poller = new BQPoll.Poller[F](pollConfig)
  private val bqRead = BigQueryRead.fromClient[F](client, uri"https://bigquerystorage.googleapis.com")
  import Http4sImplicits.*

  type Job = googleapis.bigquery.Job

  override protected[bigquery] def synchronousQueryExecute(
      jobId: BQJobId,
      query: BQSqlFrag,
      legacySql: Boolean,
      logStream: Boolean): Resource[F, (Schema, Stream[F, GenericRecord])] = {
    def openServerStreams(job: Job, numStreams: Int): F[(ReadSession, List[Stream[F, ReadRowsResponse]])] =
      for {
        tempTable <- F.delay(
          job.configuration
            .flatMap(
              _.query
                .flatMap(_.destinationTable)
                .flatMap(TableHelper.fromTableReference))
            .getOrElse(throw new IllegalStateException(s"Unable to get destination table from ${job.jobReference}")))
        request = CreateReadSessionRequest.defaultInstance
          .withParent("projects/" + tempTable.dataset.project.value)
          .withReadSession(
            ReadSession.defaultInstance.withTable(tempTable.asPathString).withDataFormat(DataFormat.AVRO))
          .withMaxStreamCount(numStreams)
          .withPreferredMinStreamCount(1)

        session <- bqRead.createReadSession(
          request,
          Headers(
            "content-type" -> "application/grpc",
            "user-agent" -> "http4s-bigquery",
            "x-goog-request-params" -> s"read_session.table=${Uri.Path.Segment(tempTable.asPathString).encoded}"
          )
        )
        serverStreams = session.streams.toList.map { streamN =>
          bqRead.readRows(
            ReadRowsRequest.defaultInstance.withReadStream(streamN.name),
            Headers(
              "content-type" -> "application/grpc",
              "user-agent" -> "http4s-bigquery",
              "x-goog-request-params" -> s"read_stream=${Uri.Path.Segment(streamN.name).encoded}")
          )
        }
      } yield (session, serverStreams)

    def rows(datumReader: GenericDatumReader[GenericRecord]): Pipe[F, ReadRowsResponse, GenericRecord] =
      _.flatMap(res =>
        Stream.chunk(
          res.rows.avroRows
            .map { rows =>
              val b = Vector.newBuilder[GenericRecord]

              val decoder =
                DecoderFactory.get.binaryDecoder(rows.serializedBinaryRows.toByteArray, null)

              while (!decoder.isEnd)
                b += datumReader.read(null, decoder)
              Chunk.from(b.result())
            }
            .getOrElse(Chunk.empty)))

    val run = for {
      job <- submitQueryImpl(jobId, query, legacySql, None, None)
      tuple <- openServerStreams(job.job, 4)
      (session, streams) = tuple
      schema <- F.delay(
        new Schema.Parser()
          .parse(session.schema.avroSchema.map(_.schema).getOrElse(sys.error("No avro schema from session"))))
      datumReader = new GenericDatumReader[GenericRecord](schema)
      baseStream = streams.map(_.through(rows(datumReader))).reduceOption(_.merge(_)).getOrElse(Stream.empty)
      rowStream =
        if (logStream) {
          baseStream.chunks
            .through(StreamUtils.logChunks(logger, None, "downloading"))
            .flatMap(Stream.chunk)

        } else baseStream
    } yield schema -> rowStream

    Resource.eval(run)
  }

  override def loadToHashedPartition[A](
      jobId: BQJobId,
      table: BQTableDef.Table[Long],
      stream: fs2.Stream[F, A],
      logStream: Boolean,
      chunkSize: Int)(implicit hashedEncoder: HashedPartitionEncoder[A]): F[Option[BQJobStatistics.Load]] = {
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
  override def loadJson[A: Encoder, P: TableOps](
      jobId: BQJobId,
      table: BQTableDef.Table[P],
      partition: P,
      stream: fs2.Stream[F, A],
      writeDisposition: WriteDisposition,
      logStream: Boolean,
      chunkSize: Int): F[Option[BQJobStatistics.Load]] =
    loadJson(
      jobId,
      table.assertPartition(partition).asTableId,
      table.schema,
      stream,
      writeDisposition,
      logStream,
      chunkSize)

  override def submitQuery[P](
      id: BQJobId,
      query: BQSqlFrag,
      destination: Option[BQPartitionId[P]],
      writeDisposition: Option[WriteDisposition]): F[JobWithStats[Job]] =
    submitQueryImpl(
      id = id,
      query = query,
      legacySql = false,
      destination = destination,
      writeDisposition = writeDisposition)

  private def submitQueryImpl[P](
      id: BQJobId,
      query: BQSqlFrag,
      legacySql: Boolean,
      destination: Option[BQPartitionId[P]],
      writeDisposition: Option[WriteDisposition]): F[JobWithStats[Job]] = {
    val project = id.projectId.getOrElse(defaults.projectId)

    val submitted = submitJob(id) { ref =>
      val jobSpec = Job(
        jobReference = Some(ref),
        configuration = Some(
          JobConfiguration(
            jobType = Some("QUERY"),
            query = Some(JobConfigurationQuery(
              query = Some(query.asStringWithUDFs),
              useLegacySql = Some(legacySql),
              writeDisposition = writeDisposition.map(_.name),
              destinationTable = destination.map(p => TableHelper.toTableReference(p.asTableId))
            )),
            labels = Some(id.labels.value)
          ))
      )
      jobsClient.insert(project.value)(jobSpec).map(_.some).recoverWith {
        case err: GoogleError if err.code.contains(Status.Conflict.code) =>
          OptionT.fromOption[F](ref.jobId).semiflatMap(id => jobsClient.get(project.value, id)).value
      }
    }
    def toJobStats(job: Job) =
      for {
        id <- JobHelper.jobId(job)
        stats <- job.statistics
        qstats <- JobHelper.toStats(id, stats)
      } yield JobWithStats(job, qstats)

    OptionT(submitted)
      .subflatMap(toJobStats)
      .getOrElseF(F.raiseError(new Exception(s"Unexpected: got no job after submitting ${id.name}")))
  }

  override def extract(id: BQJobId, extract: BQTableExtract): F[BQJobStatistics.Extract] = {
    val project = id.projectId.getOrElse(defaults.projectId)

    val csv = extract.format match {
      case c: BQTableExtract.Format.CSV => Some(c)
      case _ => None
    }

    def submit(ref: JobReference) = {
      val jobSpec = Job(
        jobReference = Some(ref),
        configuration = Some(
          JobConfiguration(
            jobType = Some("EXTRACT"),
            labels = Some(id.labels.value),
            extract = Some(JobConfigurationExtract(
              destinationUris = Some(extract.urls.map(_.value)),
              useAvroLogicalTypes = extract.format match {
                case BQTableExtract.Format.AVRO(logicalTypes) => Some(logicalTypes)
                case _ => None
              },
              sourceModel = None,
              printHeader = csv match {
                case Some(BQTableExtract.Format.CSV(_, printHeader)) => Some(printHeader)
                case _ => None
              },
              compression = Some(extract.compression.value),
              fieldDelimiter = csv match {
                case Some(BQTableExtract.Format.CSV(delimiter, _)) => Some(delimiter)
                case _ => None
              },
              destinationUri = None,
              destinationFormat = Some(extract.format.value),
              modelExtractOptions = None,
              sourceTable = Some(TableHelper.toTableReference(extract.source))
            ))
          ))
      )
      jobsClient.insert(project.value)(jobSpec).map(_.some).recoverWith {
        case err: GoogleError if err.code.contains(Status.Conflict.code) =>
          OptionT.fromOption[F](ref.jobId).semiflatMap(id => jobsClient.get(project.value, id)).value
      }
    }
    def toJobStats(job: Job) =
      for {
        id <- JobHelper.jobId(job)
        stats <- job.statistics
        qstats <- JobHelper.toExtractStats(id, stats)
      } yield qstats

    val submitted = OptionT(
      submitJobWithPoller(id)(submit)(
        new BQPoll.Poller[F](pollConfig.copy(maxDuration = extract.timeout.getOrElse(pollConfig.maxDuration)))))

    submitted
      .subflatMap(toJobStats)
      .getOrElseF(F.raiseError(new Exception(s"Unexpected: got no job after submitting ${id.name}")))
  }

  override def dryRun(id: BQJobId, query: BQSqlFrag): F[BQJobStatistics.Query] = {
    val submitted = OptionT
      .liftF(
        freshJobReference(id)
          .flatMap { ref =>
            val project = id.projectId.getOrElse(defaults.projectId)
            val jobSpec = Job(
              jobReference = Some(ref),
              configuration = Some(
                JobConfiguration(
                  query = Some(
                    JobConfigurationQuery(
                      query = Some(query.asStringWithUDFs),
                      useLegacySql = Some(false)
                    )),
                  labels = Some(id.labels.value),
                  dryRun = Some(true),
                  jobType = Some("QUERY")
                ))
            )
            jobsClient.insert(project.value)(jobSpec)
          })

    def toJobStats(job: Job) =
      for {
        id <- JobHelper.jobId(job)
        stats <- job.statistics
        qstats <- JobHelper.toQueryStats(id, stats)
      } yield qstats

    submitted
      .subflatMap(toJobStats)
      .getOrElseF(F.raiseError(new Exception(s"Unexpected: got no job after submitting ${id.name}")))
  }

  def submitJob(jobId: BQJobId)(runRef: JobReference => F[Option[Job]]): F[Option[Job]] =
    submitJobWithPoller(jobId)(runRef)(poller)

  private def submitJobWithPoller(jobId: BQJobId)(runRef: JobReference => F[Option[Job]])(
      poller: BQPoll.Poller[F]): F[Option[Job]] = {
    val project = jobId.projectId.getOrElse(defaults.projectId)
    val location = jobId.locationId.getOrElse(defaults.locationId)

    def run(ref: JobReference): F[Option[Job]] =
      runRef(ref).flatMap {
        case Some(job) =>
          poller
            .poll[Job](
              runningJob = job,
              retry = OptionT
                .fromOption[F]
                (job.jobReference.flatMap(_.jobId))
                .flatMapF(id =>
                  jobsClient
                    .get(project.value, id, query = JobsClient.GetParams(location = Some(location.value)))
                    .map(_.some)
                    .recoverWith {
                      case err: GoogleError if err.code.contains(Status.NotFound.code) =>
                        F.pure(None)
                    })
                .value
            )
            .flatMap {
              case BQPoll.Failed(error) => F.raiseError[Option[Job]](error)
              case BQPoll.Success(job) => F.pure(job.some)
            }
            .guaranteeCase {
              case Outcome.Errored(e) =>
                logger.warn(e)(show"${job.asJson.noSpaces} failed")
              case Outcome.Canceled() =>
                logger.warn(show"${job.asJson.noSpaces} cancelled")
              case Outcome.Succeeded(_) =>
                logger.debug(show"${job.asJson.noSpaces} succeeded")
            }
        case None => F.pure(None)
      }

    freshJobReference(jobId).flatMap(run)
  }

  override def createTempTable[Param](
      table: BQTableDef.Table[Param],
      tmpDataset: BQDataset.Ref,
      expirationDuration: Option[FiniteDuration]): F[BQTableDef.Table[Param]] =
    F.delay(
      table.copy(tableId = BQTableId(
        tmpDataset,
        table.tableId.tableName + UUID.randomUUID().toString
      )))
      .flatMap { tmp =>
        val duration = expirationDuration.getOrElse(1.hour)
        Clock[F].realTime.map(realtime => realtime + duration).flatMap { exp =>
          val converted = TableHelper
            .toGoogle(tmp, None)
            .copy(expirationTime = Some(exp))
          tableClient
            .insert(table.tableId.dataset.project.value, table.tableId.dataset.id)(converted)
            .as(tmp)
        }
      }

  override def createTempTableResource[Param](
      table: BQTableDef.Table[Param],
      tmpDataset: BQDataset.Ref): Resource[F, BQTableDef.Table[Param]] =
    Resource.make(createTempTable(table, tmpDataset))(tmp =>
      tableClient.delete(tmp.tableId.dataset.project.value, tmp.tableId.dataset.id, tmp.tableId.tableName).void)

  private def loadJson[A: Encoder](
      id: BQJobId,
      tableId: BQTableId,
      schema: BQSchema,
      stream: fs2.Stream[F, A],
      writeDisposition: WriteDisposition,
      logStream: Boolean,
      chunkSize: Int
  ): F[Option[BQJobStatistics.Load]] = {
    val submitted = submitJob(id) { ref =>
      val jobSpec = Job(
        jobReference = Some(ref),
        configuration = Some(
          JobConfiguration(
            labels = Some(id.labels.value),
            jobType = Some("LOAD"),
            load = Some(JobConfigurationLoad(
              destinationTable = Some(TableHelper.toTableReference(tableId)),
              sourceUris = Some(Nil),
              schema = Some(SchemaHelper.toTableSchema(schema)),
              writeDisposition = Some(writeDisposition.name),
              sourceFormat = Some("NEWLINE_DELIMITED_JSON")
            ))
          ))
      )
      resumableUploadUri(id.projectId.getOrElse(defaults.projectId), jobSpec).flatMap(uri =>
        upload(uri, stream, logStream, chunkSize))
    }
    def toJobStats(job: Job) =
      for {
        id <- JobHelper.jobId(job)
        stats <- job.statistics
        lstats <- JobHelper.toLoadStats(id, stats)
      } yield lstats

    OptionT(submitted).subflatMap(toJobStats).value
  }

  private def freshJobReference(id: BQJobId): F[JobReference] = {
    val withDefaults = id.withDefaults(Some(defaults))

    F.delay(
      JobReference(
        jobId = Some(s"${withDefaults.name}-${UUID.randomUUID}"),
        location = withDefaults.locationId.map(_.value),
        projectId = Some(withDefaults.projectId.getOrElse(defaults.projectId).value)
      )
    )
  }

  private def resumableUploadUri(projectId: ProjectId, job: Job): F[Uri] = {
    import org.http4s.headers.Location
    val uploadUri = {
      val base = jobsClient.baseUri
      base.withPath(path"/upload".addSegments(base.path.segments)) / "projects"
    }

    client
      .run(
        Request[F](
          httpVersion = HttpVersion.`HTTP/2`,
          method = Method.POST,
          uri = (uploadUri / projectId.value / "jobs")
            .withQueryParam("uploadType", "resumable")
        )
          .withEntity(job)
          .putHeaders("X-Upload-Content-Value" -> "application/octet-stream"))
      .use ( res =>

        res.headers.get[Location] match {
          case Some(value ) => F.pure(value.uri)
          case None =>
            res
              .as[GoogleError]
              .attempt
              .flatMap(err =>
                F.raiseError[Uri](
                  new IllegalStateException(
                    s"Not possible to create a upload uri for ${job.asJson.dropNullValues.noSpaces}",
                    err.merge)))
        }
      )
  }

  private def upload[A: Encoder](uri: Uri, stream: Stream[F, A], logStream: Boolean, chunkSize: Int): F[Option[Job]] = {
    val followRedirects = org.http4s.client.middleware
      .FollowRedirect(1)(client)

    def uploadChunk(chunk: Chunk[Byte], destOffset: Long): F[(Long, Option[Job])] = {
      val limit = destOffset + chunk.size
      val last = chunk.size < chunkSize

      val range = {
        val range = new StringBuilder("bytes ")
        range.append(destOffset).append('-').append(limit - 1).append('/')
        if (last) range.append(limit)
        else range.append('*')
        range.toString()
      }

      followRedirects
        .run(
          Request[F](uri = uri, method = Method.PUT)
            .withBodyStream(Stream.chunk(chunk))
            .putHeaders(
              `Content-Type`(MediaType.application.`octet-stream`),
              Header.Raw(ci"Content-Range", range)
            ))
        .use { res =>
          if (last && res.status == Status.Ok) {
            res.as[Job].map(x => limit -> x.some)
          } else if (res.status.responseClass == Status.ClientError) {
            res
              .as[GoogleError]
              .attempt
              .flatMap(err => F.raiseError(new IllegalStateException(s"Unable to upload to ${uri}", err.merge)))

          } else F.pure(limit -> none[Job])
        }
    }

    stream
      .through(StreamUtils.toLineSeparatedJsonBytes(chunkSize))
      .evalMapAccumulate(0L) { case (state, chunk) =>
        if (logStream) {
          val msg = List(
            "uploading",
            chunk.size.toLong.toString,
            s"accumulated ${state + chunk.size}"
          ).mkString(" ")
          logger.info(msg) >> uploadChunk(chunk, state)
        } else uploadChunk(chunk, state)
      }
      .map(_._2)
      .unNone
      .compile
      .last
  }
}

object Http4sQueryClient {
  def fromClient[F[_]: Async: LoggerFactory](
      client: Client[F],
      defaults: BQClientDefaults,
      pollConfig: QueryClient.PollConfig): Http4sQueryClient[F] =
    new Http4sQueryClient[F](client, defaults, pollConfig)
}
