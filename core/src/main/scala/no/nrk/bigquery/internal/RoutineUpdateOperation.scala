/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package internal

import cats.Eq

object RoutineUpdateOperation {
  implicit val eqUDF: Eq[UDF.Persistent[?]] = Eq.instance { (a, b) =>
    a.name == b.name &&
    a.params == b.params &&
    a.body.asFragment.asString == b.body.asFragment.asString &&
    a.returnType == b.returnType
  }

  implicit val eqTVF: Eq[TVF[?, ?]] = Eq.instance { (a, b) =>
    a.name == b.name &&
    a.params == b.params &&
    a.description == b.description &&
    conforms.onlyTypes(a.schema, b.schema).isEmpty &&
    a.partitionType == b.partitionType &&
    a.query.asString == b.query.asString
  }

  def from[R](
      routine: BQPersistentRoutine.Unknown,
      maybeExisting: Option[ExistingRoutine[R]]
  ): UpdateOperation[R, Nothing] =
    maybeExisting match {
      case None =>
        routine match {
          case tvf: TVF[Any, ?] =>
            UpdateOperation.CreateTvf(tvf)
          case udf: UDF.Persistent[?] =>
            UpdateOperation.CreatePersistentUdf(udf)
        }
      case Some(remoteValue) =>
        (routine, remoteValue.our) match {
          case (local: TVF[?, ?], remote: TVF[?, ?]) =>
            val patched = remote.withParitionType(local.partitionType)
            if (eqTVF.eqv(local, patched)) {
              UpdateOperation.Noop(PersistentRoutineOperationMeta(remote, routine))
            } else {
              conforms.onlyTypes(local.schema, remote.schema) match {
                case Some(illegalFields) =>
                  UpdateOperation.IllegalSchemaExtension(
                    PersistentRoutineOperationMeta(remote, routine),
                    illegalFields.mkString(", "))
                case None =>
                  UpdateOperation.UpdateTvf(remoteValue, local)

              }
            }
          case (local: UDF.Persistent[?], remote: UDF.Persistent[?]) =>
            if (eqUDF.eqv(local, remote)) {
              UpdateOperation.Noop(PersistentRoutineOperationMeta(remote, routine))
            } else {
              UpdateOperation.UpdatePersistentUdf(remoteValue, local)
            }
          case (local, remote) =>
            UpdateOperation.Illegal(
              PersistentRoutineOperationMeta(local, remote),
              s"Cannot convert from '${remote.getClass.getSimpleName}' to '${local.getClass.getSimpleName}'"
            )
        }
    }
}
