/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.{Applicative, MonadThrow, Show}
import cats.syntax.all._
import com.google.cloud.bigquery.{Option => _, _}
import no.nrk.bigquery.internal.{TableUpdateOperation, UdfUpdateOperation}
import org.typelevel.log4cats.LoggerFactory

sealed trait OperationMeta {
  def identifier: String
}
case class TableDefOperationMeta(
    existingRemoteTable: TableInfo,
    localTableDef: BQTableDef[Any]
) extends OperationMeta {
  def identifier: String = existingRemoteTable.getTableId.toString
}
case class UdfOperationMeta(
    routine: RoutineInfo,
    persistentUdf: UDF.Persistent[_]
) extends OperationMeta {
  override def identifier: String = persistentUdf.name.asString
}

sealed trait UpdateOperation
object UpdateOperation {
  case class Noop(meta: OperationMeta) extends UpdateOperation

  sealed trait Success extends UpdateOperation

  /** @param maybePatchedTable
    *   It's not allowed to provide schema when creating a view
    */
  case class CreateTable(
      localTableDef: BQTableDef[Any],
      table: TableInfo,
      maybePatchedTable: Option[TableInfo]
  ) extends Success

  case class UpdateTable(
      existingRemoteTable: TableInfo,
      localTableDef: BQTableDef.Table[Any],
      table: TableInfo
  ) extends Success

  case class RecreateView(
      existingRemoteTable: TableInfo,
      localTableDef: BQTableDef.ViewLike[Any],
      create: CreateTable
  ) extends Success

  case class CreatePersistentUdf(
      persistentUdf: UDF.Persistent[_],
      routine: RoutineInfo
  ) extends Success

  case class UpdatePersistentUdf(
      persistentUdf: UDF.Persistent[_],
      routine: RoutineInfo
  ) extends Success

  sealed trait Error extends UpdateOperation

  case class Illegal(meta: OperationMeta, reason: String) extends Error
  case class UnsupportedPartitioning(meta: OperationMeta, msg: String) extends Error
  case class IllegalSchemaExtension(meta: OperationMeta, reason: String) extends Error

}

class EnsureUpdated[F[_]](
    bqClient: BigQueryClient[F]
)(implicit F: MonadThrow[F], lf: LoggerFactory[F]) {
  private val logger = lf.getLogger

  private def bqFormatTableId(tableId: TableId): BQSqlFrag = BQSqlFrag(
    s"`${tableId.getProject}.${tableId.getDataset}.${tableId.getTable}`"
  )

  private implicit val showTableId: Show[TableId] = Show.show(tid => s"`${bqFormatTableId(tid)}`")

  def check(template: BQTableDef[Any]): F[UpdateOperation] =
    bqClient.getTable(template.tableId).map { maybeExisting =>
      TableUpdateOperation.from(template, maybeExisting)
    }

  def check(persistentUdf: UDF.Persistent[_]): F[UpdateOperation] =
    bqClient.getRoutine(persistentUdf.name).map { maybeExisting =>
      UdfUpdateOperation.from(persistentUdf, maybeExisting)
    }

  def perform(updateOperation: UpdateOperation): F[Unit] =
    updateOperation match {
      case UpdateOperation.Noop(_) =>
        Applicative[F].unit

      case UpdateOperation.CreateTable(to, table, maybePatchedTable) =>
        for {
          _ <- logger.warn(show"Creating ${table.getTableId} of type ${to.getClass.getSimpleName}")
          _ <- bqClient.create(table)
          _ <- maybePatchedTable match {
            case Some(patchedTable) => bqClient.update(patchedTable).void
            case None => Applicative[F].unit
          }
        } yield ()

      case UpdateOperation.UpdateTable(from, to, table) =>
        val msg =
          show"Updating ${table.getTableId} of type ${to.getClass.getSimpleName} from ${from.toString}, to ${to.toString}"
        logger.warn(msg) >> bqClient.update(table).void

      case UpdateOperation.CreatePersistentUdf(udf, routine) =>
        for {
          _ <- logger.warn(show"Creating ${udf.name} of type PersistentUdf")
          _ <- bqClient.create(routine)
        } yield ()

      case UpdateOperation.UpdatePersistentUdf(udf, routine) =>
        for {
          _ <- logger.warn(show"Updating ${udf.name} of type PersistentUdf")
          _ <- bqClient.update(routine)
        } yield ()

      case UpdateOperation.RecreateView(from, to, createNew) =>
        val msg =
          show"Recreating ${to.tableId} of type ${to.getClass.getSimpleName} from ${from.toString}, to ${to.toString}"
        for {
          _ <- logger.warn(msg)
          _ <- bqClient.delete(createNew.localTableDef.tableId)
          updated <- perform(createNew)
        } yield updated

      case UpdateOperation.Illegal(meta, reason) =>
        MonadThrow[F].raiseError(
          new RuntimeException(show"Illegal update of ${meta.identifier}: $reason")
        )

      case UpdateOperation.UnsupportedPartitioning(meta, reason) =>
        MonadThrow[F].raiseError(
          new RuntimeException(show"Illegal change of partition schema for ${meta.identifier}. $reason"))

      case UpdateOperation.IllegalSchemaExtension(meta, reason) =>
        MonadThrow[F].raiseError(
          new RuntimeException(show"Invalid table update of ${meta.identifier}: $reason")
        )
    }
}
