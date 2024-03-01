/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

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

case class ExistingTable[T](our: BQTableDef[Any], table: T)
case class ExistingRoutine[R](our: BQPersistentRoutine.Unknown, routine: R)

sealed trait UpdateOperation[+R, +T]
object UpdateOperation {
  case class Noop(meta: OperationMeta) extends UpdateOperation[Nothing, Nothing]

  sealed trait Success[+R, +T] extends UpdateOperation[R, T]

  /** @param patched
    *   It's not allowed to provide schema when creating a view
    */
  case class CreateTable(local: BQTableDef[Any], patched: Option[BQTableDef[Any]]) extends Success[Nothing, Nothing]

  case class UpdateTable[T](existing: ExistingTable[T], local: BQTableDef.Table[Any]) extends Success[Nothing, T]

  case class RecreateView[T](
      existingRemoteTable: ExistingTable[T],
      localTableDef: BQTableDef.ViewLike[Any],
      create: CreateTable)
      extends Success[Nothing, T]

  case class CreateTvf(tvf: TVF[Any, ?]) extends Success[Nothing, Nothing]

  case class UpdateTvf[R](existing: ExistingRoutine[R], tvf: TVF[Any, ?]) extends Success[R, Nothing]

  case class CreatePersistentUdf(udf: UDF.Persistent[?]) extends Success[Nothing, Nothing]
  case class UpdatePersistentUdf[R](existing: ExistingRoutine[R], udf: UDF.Persistent[?]) extends Success[R, Nothing]

  sealed trait Error extends UpdateOperation[Nothing, Nothing]

  case class Illegal(meta: OperationMeta, reason: String) extends Error
  case class UnsupportedPartitioning(meta: OperationMeta, msg: String) extends Error
  case class IllegalSchemaExtension(meta: OperationMeta, reason: String) extends Error

}
