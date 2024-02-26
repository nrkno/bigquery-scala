/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.internal

import com.google.cloud.bigquery.{Option as _, *}
import no.nrk.bigquery.UDF.Body
import no.nrk.bigquery.{
  BQField,
  BQPersistentRoutine,
  BQRoutine,
  BQType,
  PersistentRoutineOperationMeta,
  TVF,
  UDF,
  UpdateOperation
}

import scala.jdk.CollectionConverters.*

object RoutineUpdateOperation {

  private val UdfRoutineType = "SCALAR_FUNCTION"
  private val TvfRoutineType = "TABLE_VALUED_FUNCTION"

  def from(
      routine: BQPersistentRoutine[?, ?],
      maybeExisting: Option[RoutineInfo]
  ): UpdateOperation = maybeExisting match {
    case None =>
      routine match {
        case tvf: TVF[Any, ?] =>
          UpdateOperation.CreateTvf(tvf, createTvfRoutineInfo(tvf))
        case udf: UDF.Persistent[?] =>
          UpdateOperation.CreatePersistentUdf(udf, createUdfRoutineInfo(udf))
      }
    case Some(value) =>
      val tpe = routine match {
        case _: TVF[Any, ?] => TvfRoutineType
        case _: UDF.Persistent[?] => UdfRoutineType
      }
      if (value.getRoutineType != tpe)
        UpdateOperation.Illegal(
          PersistentRoutineOperationMeta(value, routine),
          s"Routine type ${value.getRoutineType} did not match expected routine $tpe")
      else {
        val recreated = recreateRoutine(value)
        val routineInfo = routine match {
          case tvf: TVF[Any, ?] => createTvfRoutineInfo(tvf)
          case udf: UDF.Persistent[?] => createUdfRoutineInfo(udf)
        }
        if (recreated == routineInfo)
          UpdateOperation.Noop(PersistentRoutineOperationMeta(value, routine))
        else {
          routine match {
            case tvf: TVF[Any, ?] => UpdateOperation.UpdateTvf(tvf, routineInfo)
            case udf: UDF.Persistent[?] => UpdateOperation.UpdatePersistentUdf(udf, routineInfo)
          }
        }
      }
  }

  private def createTvfRoutineInfo(tvf: TVF[Any, ?]) =
    RoutineInfo
      .newBuilder(toRoutineId(tvf.name))
      .setRoutineType(TvfRoutineType)
      .setArguments(tvf.params.unsized.toList.map(toRoutineArgs).asJava)
      .setLanguage("SQL")
      .setBody(tvf.query.asString)
      // function not public in Builder: .setDescription(tvf.description.orNull)
      .setReturnTableType(
        StandardSQLTableType
          .newBuilder()
          .setColumns(
            tvf.schema.fields
              .map(field => StandardSQLField.newBuilder(field.name, toSqlDataType(BQType.fromField(field))).build())
              .asJava)
          .build()
      )
      .setImportedLibraries(List.empty.asJava)
      .build()

  private def createUdfRoutineInfo(udf: UDF.Persistent[?]) = {
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
          .setImportedLibraries(List.empty.asJava)
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
      .setRoutineType(r.getRoutineType)
      .setArguments(Option(r.getArguments).getOrElse(List.empty.asJava))
      .setReturnType(r.getReturnType)
      .setReturnTableType(r.getReturnTableType)
      .setLanguage(r.getLanguage)
      .setBody(r.getBody)
      .setImportedLibraries(Option(r.getImportedLibraries).getOrElse(List.empty.asJava))
      .build()

  private def toRoutineId(id: BQPersistentRoutine.Id) =
    RoutineId.of(id.dataset.project.value, id.dataset.id, id.name.value)

  private def toRoutineArgs(param: BQRoutine.Param): RoutineArgument =
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
