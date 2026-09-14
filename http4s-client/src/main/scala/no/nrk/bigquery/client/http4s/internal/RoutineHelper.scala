/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package client.http4s.internal

import cats.syntax.all.*
import googleapis.bigquery.*
import no.nrk.bigquery.util.ToSized

object RoutineHelper {
  def fromGoogle(routine: Routine): Either[String, BQPersistentRoutine.Unknown] =
    for {
      ref <- routine.routineReference.toRight("No routine reference defined")
      tpe <- routine.routineType.toRight("No routine type defined")
      converted <- tpe match {
        case RoutineRoutineType.SCALAR_FUNCTION => toUDF(routine, ref)
        case RoutineRoutineType.TABLE_VALUED_FUNCTION => toTVF(routine, ref)
        case x => Left(s"routine type '${x.value}' is not supported")
      }

    } yield converted

  def toGoogle(routine: BQPersistentRoutine.Unknown, existing: Option[Routine]) =
    routine match {
      case TVF(name, _, params, query, schema, description) =>
        val patching = existing.getOrElse(
          Routine(
            routineReference = Some(
              RoutineReference(
                projectId = Some(name.dataset.project.value),
                datasetId = Some(name.dataset.id),
                routineId = Some(name.name.value))),
            language = Some(RoutineLanguage.SQL),
            routineType = Some(RoutineRoutineType.TABLE_VALUED_FUNCTION)
          ))

        patching.copy(
          arguments = Some(
            params.unsized
              .map(p => Argument(name = Some(p.name.value), dataType = p.maybeType.map(SchemaHelper.fromBQType)))
              .toList),
          definitionBody = Some(query.asString),
          returnTableType = Some(SchemaHelper.toTableType(schema)),
          description = description
        )

      case UDF.Persistent(name, params, body, returnType, description) =>
        val (lang, bodyString, imports) = body match {
          case UDF.Body.Sql(bd) => (RoutineLanguage.SQL, bd.asString, None)
          case UDF.Body.Js(bd, imports) => (RoutineLanguage.JAVASCRIPT, bd, Some(imports))
        }
        val patching = existing.getOrElse(
          Routine(
            routineReference = Some(
              RoutineReference(
                projectId = Some(name.dataset.project.value),
                datasetId = Some(name.dataset.id),
                routineId = Some(name.name.value))),
            language = Some(lang),
            routineType = Some(RoutineRoutineType.SCALAR_FUNCTION)
          ))

        patching.copy(
          arguments = Some(
            params.unsized
              .map(p => Argument(name = Some(p.name.value), dataType = p.maybeType.map(SchemaHelper.fromBQType)))
              .toList),
          definitionBody = Some(bodyString),
          importedLibraries = imports,
          returnType = returnType.map(SchemaHelper.fromBQType),
          description = description
        )
    }

  def toUdfId(ref: RoutineReference) =
    (ref.projectId, ref.datasetId, ref.routineId).mapN((a, b, c) =>
      UDF.UDFId.PersistentId(BQDataset.Ref(ProjectId.unsafeFromString(a), b), Ident(c)))

  def toTVFId(ref: RoutineReference) =
    (ref.projectId, ref.datasetId, ref.routineId).mapN((a, b, c) =>
      TVF.TVFId(BQDataset.Ref(ProjectId.unsafeFromString(a), b), Ident(c)))

  def toParam(argument: Argument) =
    (argument.name, argument.dataType).mapN((name, dt) => BQRoutine.Param(Ident(name), SchemaHelper.toBQType(dt)))

  def toUDF(routine: Routine, ref: RoutineReference) =
    for {
      id <- toUdfId(ref).toRight("Not possible to create UDF.UDFId.PersistentId")
      params = routine.arguments.getOrElse(Nil).flatMap(toParam)
      lang <- routine.language.toRight("No language defined")
      body <- lang match {
        case RoutineLanguage.SQL =>
          Right(UDF.Body.Sql(routine.definitionBody.map(BQSqlFrag.Frag.apply).getOrElse(BQSqlFrag.Empty)))
        case RoutineLanguage.JAVASCRIPT =>
          Right(
            UDF.Body.Js(
              routine.definitionBody.getOrElse(""),
              routine.importedLibraries.getOrElse(Nil)
            ))
        case x => Left(s"Unsupported language: '$x'")
      }
    } yield UDF.Persistent(
      id,
      ToSized(params),
      body,
      routine.returnType.flatMap(SchemaHelper.toBQType),
      routine.description
    )

  def toTVF(routine: Routine, ref: RoutineReference) =
    for {
      id <- toTVFId(ref).toRight("Not possible to create TVF.TVFId")
      params = routine.arguments.getOrElse(Nil).flatMap(toParam)
    } yield TVF(
      id,
      BQPartitionType.NotPartitioned,
      ToSized(params),
      routine.definitionBody.map(BQSqlFrag.Frag.apply).getOrElse(BQSqlFrag.Empty),
      routine.returnTableType.flatMap(SchemaHelper.fromTableType).getOrElse(BQSchema.of())
    )
}
