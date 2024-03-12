/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.effect.{Concurrent, Resource}
import fs2.Stream
import io.circe.Encoder
import no.nrk.bigquery.util.StreamUtils
import org.apache.avro
import org.apache.avro.generic.GenericRecord

import scala.concurrent.duration.*

trait QueryClient[F[_]] {
  type Job
  def synchronousQuery[A](
      jobId: BQJobId,
      query: BQQuery[A]
  )(implicit M: Concurrent[F]): Stream[F, A] =
    synchronousQuery(jobId, query, legacySql = false)

  def synchronousQuery[A](
      jobId: BQJobId,
      query: BQQuery[A],
      legacySql: Boolean
  )(implicit M: Concurrent[F]): Stream[F, A] =
    synchronousQuery(jobId, query, legacySql, logStream = true)

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
      logStream: Boolean
  )(implicit M: Concurrent[F]): Stream[F, A] =
    Stream
      .resource(
        synchronousQueryExecute(
          jobId,
          query.sql,
          legacySql,
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

  protected[bigquery] def synchronousQueryExecute(
      jobId: BQJobId,
      query: BQSqlFrag,
      legacySql: Boolean,
      logStream: Boolean
  ): Resource[F, (avro.Schema, Stream[F, GenericRecord])]

  def loadJson[A: Encoder, P: TableOps](
      jobId: BQJobId,
      table: BQTableDef.Table[P],
      partition: P,
      stream: fs2.Stream[F, A],
      writeDisposition: WriteDisposition
  ): F[Option[BQJobStatistics.Load]] = loadJson(
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
  ): F[Option[BQJobStatistics.Load]] =
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
  )(implicit hashedEncoder: HashedPartitionEncoder[A]): F[Option[BQJobStatistics.Load]] =
    loadToHashedPartition(jobId, table, stream, logStream = false)

  def loadToHashedPartition[A](
      jobId: BQJobId,
      table: BQTableDef.Table[Long],
      stream: fs2.Stream[F, A],
      logStream: Boolean
  )(implicit hashedEncoder: HashedPartitionEncoder[A]): F[Option[BQJobStatistics.Load]] =
    loadToHashedPartition(jobId, table, stream, logStream, chunkSize = 10 * StreamUtils.Megabyte)

  def loadToHashedPartition[A](
      jobId: BQJobId,
      table: BQTableDef.Table[Long],
      stream: fs2.Stream[F, A],
      logStream: Boolean,
      chunkSize: Int
  )(implicit hashedEncoder: HashedPartitionEncoder[A]): F[Option[BQJobStatistics.Load]]

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
  ): F[Option[BQJobStatistics.Load]]

  def submitQuery[P](jobId: BQJobId, query: BQSqlFrag): F[JobWithStats[Job]] =
    submitQuery(jobId, query, None)

  def submitQuery[P](
      id: BQJobId,
      query: BQSqlFrag,
      destination: Option[BQPartitionId[P]]
  ): F[JobWithStats[Job]] =
    submitQuery(id, query, destination, None)

  def submitQuery[P](
      id: BQJobId,
      query: BQSqlFrag,
      destination: Option[BQPartitionId[P]],
      writeDisposition: Option[WriteDisposition]
  ): F[JobWithStats[Job]]

  def dryRun(
      id: BQJobId,
      query: BQSqlFrag
  ): F[BQJobStatistics.Query]

  def createTempTable[Param](
      table: BQTableDef.Table[Param],
      tmpDataset: BQDataset.Ref
  ): F[BQTableDef.Table[Param]] =
    createTempTable(table, tmpDataset, Some(1.hour))

  def createTempTable[Param](
      table: BQTableDef.Table[Param],
      tmpDataset: BQDataset.Ref,
      expirationDuration: Option[FiniteDuration]
  ): F[BQTableDef.Table[Param]]

  def createTempTableResource[Param](
      table: BQTableDef.Table[Param],
      tmpDataset: BQDataset.Ref): Resource[F, BQTableDef.Table[Param]]
}

object QueryClient {
  case class PollConfig(
      baseDelay: FiniteDuration = 3.second,
      maxDuration: FiniteDuration = 20.minutes,
      maxErrorsTolerated: Int = 10,
      maxRetryNotFound: Int = 5
  )

  type Aux[F[_], J] = QueryClient[F] {
    type Job = J
  }
}
