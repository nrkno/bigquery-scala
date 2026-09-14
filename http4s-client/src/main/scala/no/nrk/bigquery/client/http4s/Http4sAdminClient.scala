/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.client.http4s

import cats.effect.Async
import cats.syntax.all.*
import googleapis.bigquery.*
import no.nrk.bigquery.*
import no.nrk.bigquery.client.http4s.internal.{RoutineHelper, TableHelper}
import org.http4s.client.Client

class Http4sAdminClient[F[_]] private (client: Client[F])(implicit F: Async[F])
    extends BQAdminClientWithUnderlying[F, Routine, Table] {
  private val tableClient = new TablesClient[F](client)
  private val routineClient = new RoutinesClient[F](client)
  private val datasetClient = new DatasetsClient[F](client)

  private def toMaybe[A](either: Either[Throwable, A]): F[Option[A]] =
    either match {
      case Left(GoogleError(Some(404), _, _, _)) => F.pure(none[A])
      case Left(err) => F.raiseError(err)
      case Right(value) => F.pure(value.some)
    }

  override def getTableWithUnderlying(tableId: BQTableId): F[Option[ExistingTable[Table]]] =
    tableClient
      .get(tableId.dataset.project.value, tableId.dataset.id, tableId.tableName)
      .attempt
      .flatMap(toMaybe)
      .map(_.flatMap(t => TableHelper.fromGoogle(t).toOption.map(ExistingTable(_, t))))

  override def updateTableWithExisting(existing: ExistingTable[Table], table: BQTableDef[Any]): F[BQTableDef[Any]] = {
    val updated = TableHelper.toGoogle(table, Some(existing.table))

    tableClient
      .update(table.tableId.dataset.project.value, table.tableId.dataset.id, table.tableId.tableName)(updated)
      .as(table)
  }

  override def createDataset(dataset: BQDataset): F[BQDataset] =
    datasetClient
      .insert(dataset.project.value)(
        Dataset(
          datasetReference =
            Some(DatasetReference(projectId = Some(dataset.project.value), datasetId = Some(dataset.id))),
          location = dataset.location.map(_.value)
        ))
      .as(dataset)

  override def deleteDataset(dataset: BQDataset.Ref): F[Boolean] =
    datasetClient.delete(dataset.project.value, dataset.id).map(_.isSuccess)

  override def datasetsInProject(project: ProjectId): F[Vector[BQDataset]] =
    fs2.Stream
      .unfoldLoopEval(
        DatasetsClient.ListParams(maxResults = Some(100))
      )(params =>
        datasetClient
          .list(project.value, query = params)
          .map(list =>
            list.datasets.getOrElse(Nil) -> list.nextPageToken.map(tok => params.copy(pageToken = Some(tok)))))
      .flatMap(fs2.Stream.emits)
      .mapFilter(ds =>
        for {
          project <- ds.datasetReference.flatMap(_.projectId).map(ProjectId.unsafeFromString)
          id <- ds.datasetReference.flatMap(_.datasetId)
          location = ds.location.map(LocationId.apply)
        } yield BQDataset(project, id, location))
      .compile
      .toVector

  override def getDataset(dataset: BQDataset.Ref): F[Option[BQDataset]] =
    datasetClient
      .get(dataset.project.value, dataset.id)
      .attempt
      .flatMap(toMaybe)
      .map(_.flatMap(toDataset))

  private def toDataset(ds: Dataset) =
    for {
      project <- ds.datasetReference.flatMap(_.projectId).map(ProjectId.unsafeFromString)
      id <- ds.datasetReference.flatMap(_.datasetId)
      location = ds.location.map(LocationId.apply)
    } yield BQDataset(project, id, location)

  override def getTable(tableId: BQTableId): F[Option[BQTableDef[Any]]] =
    getTableWithUnderlying(tableId).map(_.map(_.our))

  override def createTable(table: BQTableDef[Any]): F[BQTableDef[Any]] =
    tableClient
      .insert(table.tableId.dataset.project.value, table.tableId.dataset.id)(TableHelper.toGoogle(table, None))
      .as(table)

  override def updateTable(table: BQTableDef[Any]): F[BQTableDef[Any]] =
    tableClient
      .update(table.tableId.dataset.project.value, table.tableId.dataset.id, table.tableId.tableName)(
        TableHelper.toGoogle(table, None))
      .as(table)

  override def deleteTable(tableId: BQTableId): F[Boolean] =
    tableClient
      .delete(tableId.dataset.project.value, tableId.dataset.id, tableId.tableName)
      .map(_.isSuccess)

  override def tablesInDataset(dataset: BQDataset.Ref): F[Vector[BQTableRef[Any]]] =
    fs2.Stream
      .unfoldLoopEval(
        TablesClient.ListParams(maxResults = Some(100))
      )(params =>
        tableClient
          .list(dataset.project.value, dataset.id, query = params)
          .map(list => list.tables.getOrElse(Nil) -> list.nextPageToken.map(tok => params.copy(pageToken = Some(tok)))))
      .flatMap(fs2.Stream.emits)
      .mapFilter(table => TableHelper.refFromGoogle(table).toOption)
      .compile
      .toVector

  override def getRoutineWithUnderlying(id: BQPersistentRoutine.Id): F[Option[ExistingRoutine[Routine]]] =
    routineClient
      .get(id.dataset.project.value, id.dataset.id, id.name.value)
      .attempt
      .flatMap(toMaybe)
      .map(_.flatMap(r => RoutineHelper.fromGoogle(r).toOption.map(ExistingRoutine(_, r))))

  override def updateRoutineWithExisting(
      existing: ExistingRoutine[Routine],
      routine: BQPersistentRoutine.Unknown): F[BQPersistentRoutine.Unknown] =
    routineClient
      .update(existing.our.name.dataset.project.value, existing.our.name.dataset.id, existing.our.name.name.value)(
        RoutineHelper.toGoogle(routine, Some(existing.routine)))
      .as(routine)

  override def getRoutine(id: BQPersistentRoutine.Id): F[Option[BQPersistentRoutine.Unknown]] =
    getRoutineWithUnderlying(id).map(_.map(_.our))

  override def createRoutine(routine: BQPersistentRoutine.Unknown): F[BQPersistentRoutine.Unknown] =
    routineClient
      .insert(routine.name.dataset.project.value, routine.name.dataset.id)(RoutineHelper.toGoogle(routine, None))
      .as(routine)

  override def updateRoutine(routine: BQPersistentRoutine.Unknown): F[BQPersistentRoutine.Unknown] =
    routineClient
      .update(routine.name.dataset.project.value, routine.name.dataset.id, routine.name.name.value)(
        RoutineHelper.toGoogle(routine, None))
      .as(routine)

  override def deleteRoutine(id: BQPersistentRoutine.Id): F[Boolean] =
    routineClient.delete(id.dataset.project.value, id.dataset.id, id.name.value).map(_.isSuccess)

  override def routinesInDataset(dataset: BQDataset.Ref): F[Vector[BQPersistentRoutine.Unknown]] =
    fs2.Stream
      .unfoldLoopEval(
        RoutinesClient.ListParams(maxResults = Some(100))
      )(params =>
        routineClient
          .list(dataset.project.value, dataset.id, query = params)
          .map(list =>
            list.routines.getOrElse(Nil) -> list.nextPageToken.map(tok => params.copy(pageToken = Some(tok)))))
      .flatMap(fs2.Stream.emits)
      .mapFilter(routine => RoutineHelper.fromGoogle(routine).toOption)
      .compile
      .toVector
}

object Http4sAdminClient {
  def fromClient[F[_]: Async](client: Client[F]): Http4sAdminClient[F] =
    new Http4sAdminClient[F](client)
}
