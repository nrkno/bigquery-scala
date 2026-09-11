/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package internal

import cats.Eq

object RoutineUpdateOperation {

  private def normalizeType(tpe: BQType): BQType =
    // make every field required
    // because getting the routine from BQ only return name and type, not mode
    // So When initializing BQType (BQType.apply(...)) when we define our routines,
    // .apply automatically adds the REQUIRED mode
    // While what we get back from the routineClieng.get from BQ does not have any
    // mode, so it gets turned into NULLABLE here in bigquery-scala
    tpe.copy(
      mode = BQField.Mode.REQUIRED,
      subFields = tpe.subFields.map { case (name, t) => (name, normalizeType(t)) }
    )

  private def normalizeParam(p: BQRoutine.Param): BQRoutine.Param =
    p.copy(maybeType = p.maybeType.map(normalizeType))

  implicit val eqUDF: Eq[UDF.Persistent[?]] = Eq.instance { (a, b) =>
    val name: Boolean = a.name == b.name
    // unsized is used because it is awkward to map on sized wrapping
    // normalize type to REQUIRED for both a and b
    // a.params == b.params &&
    val aNormalizedParams = a.params.unsized.map(normalizeParam)
    val bNormalizedParams = b.params.unsized.map(normalizeParam)
    val params: Boolean = aNormalizedParams == bNormalizedParams

    // TODO: aBodyFragment lacks an extra surrounding (), maybe because of the change we did with s.body?
    val aBodyFragment = a.body.asFragment.asString
    val bBodyFragment = b.body.asFragment.asString
    val body: Boolean = aBodyFragment == bBodyFragment

    // normalize return type to REQUIRED for both a and b
    // a.returnType == b.returnType
    val aReturnType = a.returnType.map(normalizeType)
    val bReturnType = b.returnType.map(normalizeType)
    val returnType: Boolean = aReturnType == bReturnType

    name && params && body && returnType
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
