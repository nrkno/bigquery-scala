/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package no.nrk.bigquery.internal

import com.google.cloud.bigquery.{Option => _, _}
import no.nrk.bigquery.UDF.Body
import no.nrk.bigquery.{BQField, BQType, UDF, UdfOperationMeta, UpdateOperation}

import scala.jdk.CollectionConverters._

object UdfUpdateOperation {

  private val UdfRoutineType = "SCALAR_FUNCTION"

  def from(
      udf: UDF.Persistent,
      maybeExisting: Option[RoutineInfo]
  ): UpdateOperation = maybeExisting match {
    case None =>
      UpdateOperation.CreatePersistentUdf(udf, createRoutineInfo(udf))
    case Some(value) =>
      val recreated = recreateRoutine(value)
      val routineFromUdf = createRoutineInfo(udf)
      if (value.getRoutineType != UdfRoutineType)
        UpdateOperation.Illegal(UdfOperationMeta(value, udf), s"Routine type ${value.getRoutineType} not supported")
      else if (recreated == routineFromUdf) UpdateOperation.Noop(UdfOperationMeta(value, udf))
      else UpdateOperation.UpdatePersistentUdf(udf, routineFromUdf)
  }

  private def createRoutineInfo(udf: UDF.Persistent) = {
    val baseBuilder = RoutineInfo
      .newBuilder(toRoutineId(udf.name))
      .setRoutineType(UdfRoutineType)
      .setArguments(udf.params.map(toRoutineArgs).asJava)
      .setReturnType(udf.returnType.map(toSqlDataType).orNull)

    (udf.body match {
      case Body.Sql(body) =>
        baseBuilder
          .setLanguage("SQL")
          .setBody(body.asString)
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

  private def toRoutineId(udfId: UDF.UDFId.PersistentId) =
    RoutineId.of(udfId.dataset.project.value, udfId.dataset.id, udfId.name.value)

  private def toRoutineArgs(param: UDF.Param): RoutineArgument =
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
        .setTypeKind(BQField.Type.STRUCT.name)
        .setArrayElementType(dataType)
        .build()
    else dataType
  }
}
