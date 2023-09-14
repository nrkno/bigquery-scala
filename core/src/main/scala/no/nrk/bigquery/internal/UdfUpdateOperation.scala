/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.internal

import com.google.cloud.bigquery.{Option => _, _}
import no.nrk.bigquery.UDF.Body
import no.nrk.bigquery.{
  BQField,
  BQType,
  PersistentRoutine,
  PersistentRoutineOperationMeta,
  Routine,
  TVF,
  UDF,
  UpdateOperation
}

import scala.jdk.CollectionConverters.*

object UdfUpdateOperation {

  private val UdfRoutineType = "SCALAR_FUNCTION"
  private val TvfRoutineType = "TABLE_VALUED_FUNCTION"

  def from(
      routine: PersistentRoutine[_],
      maybeExisting: Option[RoutineInfo]
  ): UpdateOperation = maybeExisting match {
    case None =>
      routine match {
        case tvf: TVF[Any, _] =>
          UpdateOperation.CreateTvf(tvf, createTvfRoutineInfo(tvf))
        case udf: UDF.Persistent[_] =>
          UpdateOperation.CreatePersistentUdf(udf, createUdfRoutineInfo(udf))
      }
    case Some(value) =>
      val tpe = routine match {
        case _: TVF[Any, _] => TvfRoutineType
        case _: UDF.Persistent[_] => UdfRoutineType
      }
      if (value.getRoutineType != tpe)
        UpdateOperation.Illegal(
          PersistentRoutineOperationMeta(value, routine),
          s"Routine type ${value.getRoutineType} did not match expected routine $tpe")
      else {
        val recreated = recreateRoutine(value)
        val routineInfo = routine match {
          case tvf: TVF[Any, _] => createTvfRoutineInfo(tvf)
          case udf: UDF.Persistent[_] => createUdfRoutineInfo(udf)
        }
        if (recreated == routineInfo)
          UpdateOperation.Noop(PersistentRoutineOperationMeta(value, routine))
        else {
          routine match {
            case tvf: TVF[Any, _] => UpdateOperation.UpdateTvf(tvf, routineInfo)
            case udf: UDF.Persistent[_] => UpdateOperation.UpdatePersistentUdf(udf, routineInfo)
          }
        }
      }
  }

  private def createTvfRoutineInfo(tvf: TVF[Any, _]) =
    RoutineInfo
      .newBuilder(toRoutineId(tvf.name))
      .setRoutineType(TvfRoutineType)
      .setArguments(tvf.params.unsized.toList.map(toRoutineArgs).asJava)
      .setLanguage("SQL")
      .setBody(tvf.query.asString)
      // function not public in Builder: .setDescription(tvf.description.orNull)
      // TODO .setReturnTableType(tvf.schema.???)
      .build()

  private def createUdfRoutineInfo(udf: UDF.Persistent[_]) = {
    val baseBuilder = RoutineInfo
      .newBuilder(toRoutineId(udf.name))
      .setRoutineType(UdfRoutineType)
      .setArguments(udf.params.unsized.map(toRoutineArgs).asJava)
      .setReturnType(udf.returnType.map(toSqlDataType).orNull)

    (udf.body match {
      case s: Body.Sql =>
        baseBuilder
          .setLanguage("SQL")
          .setBody(s.asFragment.asString)
      case Body.Js(javascriptSnippet, gsLibraryPath) =>
        baseBuilder
          .setLanguage("JAVASCRIPT")
          .setBody(javascriptSnippet)
          .setImportedLibraries(gsLibraryPath.asJava)
    }).build()
  }

  private def recreateRoutine(r: RoutineInfo) =
    RoutineInfo
      .newBuilder(r.getRoutineId)
      .setRoutineType(UdfRoutineType)
      .setArguments(r.getArguments)
      .setReturnType(r.getReturnType)
      .setLanguage(r.getLanguage)
      .setBody(r.getBody)
      .setImportedLibraries(r.getImportedLibraries)
      .build()

  private def toRoutineId(id: PersistentRoutine.PersistentRoutineId) =
    RoutineId.of(id.dataset.project.value, id.dataset.id, id.name.value)

  private def toRoutineArgs(param: Routine.Param): RoutineArgument =
    param.maybeType match {
      case Some(value) =>
        RoutineArgument
          .newBuilder()
          .setName(param.name.value)
          .setDataType(toSqlDataType(value))
          .build()
      case None =>
        RoutineArgument
          .newBuilder()
          .setName(param.name.value)
          .setKind("ANY_TYPE")
          .build()
    }

  private def toSqlDataType(bqType: BQType): StandardSQLDataType = {
    val dataType =
      if (bqType.tpe == BQField.Type.STRUCT)
        StandardSQLDataType
          .newBuilder()
          .setTypeKind(BQField.Type.STRUCT.name)
          .setStructType(
            StandardSQLStructType
              .newBuilder()
              .setFields(bqType.subFields.map { case (name, fieldBqType) =>
                StandardSQLField.newBuilder(name, toSqlDataType(fieldBqType)).build()
              }.asJava)
              .build())
          .build()
      else StandardSQLDataType.newBuilder().setTypeKind(bqType.tpe.name).build()

    if (bqType.mode == BQField.Mode.REPEATED)
      StandardSQLDataType
        .newBuilder()
        .setTypeKind(BQField.Type.ARRAY.name)
        .setArrayElementType(dataType)
        .build()
    else dataType
  }
}
