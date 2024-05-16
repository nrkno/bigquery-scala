/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.client.http4s

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import com.permutive.gcp.auth.TokenProvider
import fs2.io.file.{Files, Path}
import googleapis.bigquery.{Routine, Table}
import io.circe.Encoder
import no.nrk.bigquery.*
import no.nrk.bigquery.BQPersistentRoutine.Unknown
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.http4s.client.Client
import org.http4s.client.middleware.{Retry, RetryPolicy as HttpRetryPolicy}
import org.typelevel.log4cats.LoggerFactory
import retry.RetryPolicies.constantDelay

import scala.concurrent.duration.*

class Http4sBigQueryClient[F[_]: Async: LoggerFactory](
    client: Client[F],
    defaults: BQClientDefaults,
    pollConfig: QueryClient.PollConfig)
    extends QueryClient[F]
    with BQAdminClientWithUnderlying[F, Routine, Table] {
  val queryClient = Http4sQueryClient.fromClient(client, defaults, pollConfig)
  val adminClient = Http4sAdminClient.fromClient(client)

  override type Job = queryClient.Job

  override protected[bigquery] def synchronousQueryExecute(
      jobId: BQJobId,
      query: BQSqlFrag,
      legacySql: Boolean,
      logStream: Boolean): Resource[F, (Schema, fs2.Stream[F, GenericRecord])] =
    queryClient.synchronousQueryExecute(jobId, query, legacySql, logStream)

  override def loadToHashedPartition[A](
      jobId: BQJobId,
      table: BQTableDef.Table[Long],
      stream: fs2.Stream[F, A],
      logStream: Boolean,
      chunkSize: Int)(implicit hashedEncoder: HashedPartitionEncoder[A]): F[Option[BQJobStatistics.Load]] =
    queryClient.loadToHashedPartition(jobId, table, stream, logStream, chunkSize)

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
    queryClient.loadJson(jobId, table, partition, stream, writeDisposition, logStream, chunkSize)

  override def submitQuery[P](
      id: BQJobId,
      query: BQSqlFrag,
      destination: Option[BQPartitionId[P]],
      writeDisposition: Option[WriteDisposition]): F[JobWithStats[Job]] =
    queryClient.submitQuery(id, query, destination, writeDisposition)

  override def extract(id: BQJobId, extract: BQTableExtract): F[BQJobStatistics.Extract] =
    queryClient.extract(id, extract)

  override def dryRun(id: BQJobId, query: BQSqlFrag): F[BQJobStatistics.Query] =
    queryClient.dryRun(id, query)

  override def getTableWithUnderlying(id: BQTableId): F[Option[ExistingTable[Table]]] =
    adminClient.getTableWithUnderlying(id)

  override def updateTableWithExisting(existing: ExistingTable[Table], table: BQTableDef[Any]): F[BQTableDef[Any]] =
    adminClient.updateTableWithExisting(existing, table)

  override def getRoutineWithUnderlying(id: BQPersistentRoutine.Id): F[Option[ExistingRoutine[Routine]]] =
    adminClient.getRoutineWithUnderlying(id)

  override def updateRoutineWithExisting(existing: ExistingRoutine[Routine], routine: Unknown): F[Unknown] =
    adminClient.updateRoutineWithExisting(existing, routine)

  override def createDataset(dataset: BQDataset): F[BQDataset] =
    adminClient.createDataset(dataset)

  override def deleteDataset(dataset: BQDataset.Ref): F[Boolean] =
    adminClient.deleteDataset(dataset)

  override def datasetsInProject(project: ProjectId): F[Vector[BQDataset]] =
    adminClient.datasetsInProject(project)

  override def getDataset(dataset: BQDataset.Ref): F[Option[BQDataset]] =
    adminClient.getDataset(dataset)

  override def getTable(tableId: BQTableId): F[Option[BQTableDef[Any]]] =
    adminClient.getTable(tableId)

  override def createTable(table: BQTableDef[Any]): F[BQTableDef[Any]] =
    adminClient.createTable(table)

  override def updateTable(table: BQTableDef[Any]): F[BQTableDef[Any]] =
    adminClient.updateTable(table)

  override def deleteTable(tableId: BQTableId): F[Boolean] =
    adminClient.deleteTable(tableId)

  override def tablesInDataset(dataset: BQDataset.Ref): F[Vector[BQTableRef[Any]]] =
    adminClient.tablesInDataset(dataset)

  override def getRoutine(id: BQPersistentRoutine.Id): F[Option[Unknown]] =
    adminClient.getRoutine(id)

  override def createRoutine(routine: Unknown): F[Unknown] =
    adminClient.createRoutine(routine)

  override def updateRoutine(routine: Unknown): F[Unknown] =
    adminClient.updateRoutine(routine)

  override def deleteRoutine(id: BQPersistentRoutine.Id): F[Boolean] =
    adminClient.deleteRoutine(id)

  override def routinesInDataset(dataset: BQDataset.Ref): F[Vector[Unknown]] =
    adminClient.routinesInDataset(dataset)

  override def createTempTable[Param](
      table: BQTableDef.Table[Param],
      tmpDataset: BQDataset.Ref,
      expirationDuration: Option[FiniteDuration]): F[BQTableDef.Table[Param]] =
    queryClient.createTempTable(table, tmpDataset, expirationDuration)

  override def createTempTableResource[Param](
      table: BQTableDef.Table[Param],
      tmpDataset: BQDataset.Ref): Resource[F, BQTableDef.Table[Param]] =
    queryClient.createTempTableResource(table, tmpDataset)
}

object Http4sBigQueryClient {
  val DEFAULT_SCOPES = "https://www.googleapis.com/auth/bigquery" :: Nil

  def serviceAccount[F[_]: Async: Files](path: Path, client: Client[F]): F[TokenProvider[F]] =
    TokenProvider.serviceAccount(path, DEFAULT_SCOPES, client)

  def serviceAccountFromString[F[_]: Async: Files](input: String, client: Client[F]): F[TokenProvider[F]] =
    Files[F].tempFile.use { path =>
      fs2.Stream.emit(input).through(Files[F].writeUtf8(path)).compile.drain >>
        serviceAccount[F](path, client)
    }

  def withMiddlewares[F[_]: Async](
      client: Client[F],
      retry: HttpRetryPolicy[F] = HttpRetryPolicy[F](backoff = x => if (x < 3) Some((x * 3).seconds) else None),
      authentication: TokenProvider[F]): Client[F] =
    Retry(retry)(authentication.clientMiddleware(client))

  def defaultCached[F[_]: Async] = TokenProvider
    .cached[F]
    .safetyPeriod(4.seconds)
    .onRefreshFailure { case (_, _) => Async[F].unit }
    .onExhaustedRetries(_ => Async[F].unit)
    .onNewToken { case (_, _) => Async[F].unit }
    .retryPolicy(constantDelay[F](200.millis))

  def resource[F[_]: Async: LoggerFactory](
      client: Client[F],
      defaults: BQClientDefaults,
      authentication: TokenProvider[F],
      pollConfig: QueryClient.PollConfig = QueryClient.PollConfig(),
      retry: HttpRetryPolicy[F] = HttpRetryPolicy[F](backoff = x => if (x < 3) Some((x * 3).seconds) else None),
      configureAuth: Option[(TokenProvider.CachedBuilder[F]) => TokenProvider.CachedBuilder[F]] = None
  ): Resource[F, Http4sBigQueryClient[F]] =
    for {
      auth <- configureAuth
        .getOrElse(identity[TokenProvider.CachedBuilder[F]])(defaultCached)
        .build(authentication)
      middle = withMiddlewares(client, retry, auth)
    } yield new Http4sBigQueryClient(middle, defaults, pollConfig)
}
