/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.MonadThrow
import cats.syntax.all.*

sealed trait BQAdminClient[F[_]] {
  def createDataset(dataset: BQDataset): F[BQDataset]
  // def updateDataset(dataset: BQDataset): F[BQDataset] // not yet useful with the current model
  def deleteDataset(dataset: BQDataset.Ref): F[Boolean]

  def datasetsInProject(project: ProjectId): F[Vector[BQDataset]]
  def getDataset(dataset: BQDataset.Ref): F[Option[BQDataset]]
  final def getExistingDataset(dataset: BQDataset.Ref)(implicit M: MonadThrow[F]): F[BQDataset] =
    getExisting(dataset, getDataset, "Dataset")

  // todo: TableOptions
  // todo: TableListOptions

  def getTable(tableId: BQTableId): F[Option[BQTableDef[Any]]]

  final def getExistingTable(tableId: BQTableId)(implicit M: MonadThrow[F]): F[BQTableDef[Any]] =
    getExisting(tableId, getTable, "Table")

  def createTable(table: BQTableDef[Any]): F[BQTableDef[Any]]
  def updateTable(table: BQTableDef[Any]): F[BQTableDef[Any]]
  def deleteTable(tableId: BQTableId): F[Boolean]

  def tablesInDataset(dataset: BQDataset.Ref): F[Vector[BQTableRef[Any]]]

  def getRoutine(id: BQPersistentRoutine.Id): F[Option[BQPersistentRoutine.Unknown]]

  final def getExistingRoutine(id: BQPersistentRoutine.Id)(implicit M: MonadThrow[F]): F[BQPersistentRoutine.Unknown] =
    getExisting(id, getRoutine, "Routine")

  def createRoutine(routine: BQPersistentRoutine.Unknown): F[BQPersistentRoutine.Unknown]

  def updateRoutine(routine: BQPersistentRoutine.Unknown): F[BQPersistentRoutine.Unknown]
  def deleteRoutine(id: BQPersistentRoutine.Id): F[Boolean]
  def routinesInDataset(dataset: BQDataset.Ref): F[Vector[BQPersistentRoutine.Unknown]]

  private def getExisting[A, B](id: A, get: A => F[Option[B]], name: String)(implicit M: MonadThrow[F]): F[B] =
    get(id).flatMap {
      case None =>
        M.raiseError(new IllegalArgumentException(s"$name $id does not exists"))
      case Some(b) => M.pure(b)
    }
}

trait BQAdminClientWithUnderlying[F[_], R, T] extends BQAdminClient[F] {

  private[bigquery] def getTableWithUnderlying(id: BQTableId): F[Option[ExistingTable[T]]]
  private[bigquery] def updateTableWithExisting(existing: ExistingTable[T], table: BQTableDef[Any]): F[BQTableDef[Any]]
  private[bigquery] def getRoutineWithUnderlying(id: BQPersistentRoutine.Id): F[Option[ExistingRoutine[R]]]
  private[bigquery] def updateRoutineWithExisting(
      existing: ExistingRoutine[R],
      routine: BQPersistentRoutine.Unknown): F[BQPersistentRoutine.Unknown]

}
