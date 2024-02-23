/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import com.google.cloud.bigquery.{RoutineInfo, TableInfo}

sealed trait OperationMeta {
  def identifier: String
}
case class TableDefOperationMeta(
    remote: BQTableDef[Any],
    local: BQTableDef[Any]
) extends OperationMeta {
  def identifier: String = local.tableId.asString
}
case class PersistentRoutineOperationMeta(
    remote: BQPersistentRoutine.Unknown,
    local: BQPersistentRoutine.Unknown
) extends OperationMeta {
  override def identifier: String = local.name.asString
}

case class ExistingTable(our: BQTableDef[Any], table: TableInfo)
case class ExistingRoutine(our: BQPersistentRoutine.Unknown, routine: RoutineInfo)

sealed trait UpdateOperation
object UpdateOperation {
  case class Noop(meta: OperationMeta) extends UpdateOperation

  sealed trait Success extends UpdateOperation

  /** @param patched
    *   It's not allowed to provide schema when creating a view
    */
  case class CreateTable(local: BQTableDef[Any], patched: Option[BQTableDef[Any]]) extends Success

  case class UpdateTable(existing: ExistingTable, local: BQTableDef.Table[Any]) extends Success

  case class RecreateView(
      existingRemoteTable: ExistingTable,
      localTableDef: BQTableDef.ViewLike[Any],
      create: CreateTable)
      extends Success

  case class CreateTvf(tvf: TVF[Any, ?]) extends Success

  case class UpdateTvf(existing: ExistingRoutine, tvf: TVF[Any, ?]) extends Success

  case class CreatePersistentUdf(udf: UDF.Persistent[?]) extends Success
  case class UpdatePersistentUdf(existing: ExistingRoutine, udf: UDF.Persistent[?]) extends Success

  sealed trait Error extends UpdateOperation

  case class Illegal(meta: OperationMeta, reason: String) extends Error
  case class UnsupportedPartitioning(meta: OperationMeta, msg: String) extends Error
  case class IllegalSchemaExtension(meta: OperationMeta, reason: String) extends Error

}
