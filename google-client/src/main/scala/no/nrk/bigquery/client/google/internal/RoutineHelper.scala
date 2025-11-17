/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package client.google.internal

import com.google.cloud.bigquery.{Option as _, *}
import no.nrk.bigquery.UDF.Body
import no.nrk.bigquery.util.ToSized

import scala.jdk.CollectionConverters.*

object RoutineHelper {

  private[bigquery] val UdfRoutineType = "SCALAR_FUNCTION"
  private[bigquery] val TvfRoutineType = "TABLE_VALUED_FUNCTION"

  def fromGoogle(r: RoutineInfo): BQPersistentRoutine.Unknown = r.getRoutineType match {
    case TvfRoutineType =>
      tvfFromRoutine(r)
    case UdfRoutineType =>
      udfFromRoutine(r)
    case x => throw new UnsupportedOperationException(s"conversion of Routine of type $x is not supported")
  }

  def tvfFromRoutine(routine: RoutineInfo): TVF[Unit, ?] =
    TVF(
      TVF.TVFId(
        BQDataset.Ref(ProjectId.unsafeFromString(routine.getRoutineId.getProject), routine.getRoutineId.getDataset),
        Ident(routine.getRoutineId.getRoutine)
      ),
      BQPartitionType.NotPartitioned,
      toParams(routine),
      BQSqlFrag.Frag(routine.getBody),
      fromReturnTableType(routine.getReturnTableType),
      Option(routine.getDescription)
    )

  private def toParams(routine: RoutineInfo) =
    Option(routine.getArguments)
      .map(
        _.asScala.map(ra => BQRoutine.Param(Ident(ra.getName), Option(ra.getDataType).flatMap(SchemaHelper.typeFrom))))
      .map(buf => ToSized(buf.toList))
      .getOrElse(BQRoutine.Params.empty.asInstanceOf[util.Sized[IndexedSeq[BQRoutine.Param], util.NatUnknown]])

  def udfFromRoutine(routine: RoutineInfo): UDF.Persistent[?] =
    UDF.Persistent(
      UDF.UDFId.PersistentId(
        BQDataset.Ref(ProjectId.unsafeFromString(routine.getRoutineId.getProject), routine.getRoutineId.getDataset),
        Ident(routine.getRoutineId.getRoutine)
      ),
      toParams(routine),
      routine.getLanguage match {
        case "JAVASCRIPT" =>
          UDF.Body.Js(routine.getBody, Option(routine.getImportedLibraries).map(_.asScala.toList).getOrElse(Nil))
        case "SQL" => UDF.Body.Sql(Option(routine.getBody).map(BQSqlFrag.Frag.apply).getOrElse(BQSqlFrag.Empty))
        case x => sys.error(s"Unknown language '$x' for function ${routine.getRoutineId}")
      },
      returnType = Option(routine.getReturnType).flatMap(SchemaHelper.typeFrom),
      description = Option(routine.getDescription)
    )

  def fromReturnTableType(ttt: StandardSQLTableType): BQSchema = {

    def fromStandardField(dt: StandardSQLField): Option[BQField] = for {
      typ <- SchemaHelper.typeFrom(dt.getDataType)
    } yield BQField(dt.getName, typ.tpe, typ.mode, None, typ.asSchema.toOption.map(_.fields).getOrElse(Nil))

    BQSchema(ttt.getColumns.iterator().asScala.flatMap(f => fromStandardField(f)).toList)
  }

  def toGoogle(
      routine: BQPersistentRoutine.Unknown,
      maybeExisting: Option[RoutineInfo]
  ): RoutineInfo =
    routine match {
      case tvf: TVF[Any, ?] => createTvfRoutineInfo(tvf, maybeExisting)
      case udf: UDF.Persistent[?] => createUdfRoutineInfo(udf, maybeExisting)
    }

  private def createTvfRoutineInfo(tvf: TVF[Any, ?], maybeExisting: Option[RoutineInfo]) = {
    val builder = maybeExisting
      .map(_.toBuilder)
      .getOrElse(RoutineInfo
        .newBuilder(toRoutineId(tvf.name)))
      .setRoutineType(TvfRoutineType)
      .setArguments(tvf.params.unsized.toList.map(toRoutineArgs).asJava)
      .setLanguage("SQL")
      .setBody(tvf.query.asString)
    RoutineInfoHelper
      .setDescription(builder)(tvf.description)
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
  }

  private def createUdfRoutineInfo(udf: UDF.Persistent[?], maybeExisting: Option[RoutineInfo]) = {
    val baseBuilder = maybeExisting
      .map(_.toBuilder)
      .getOrElse(RoutineInfo
        .newBuilder(toRoutineId(udf.name)))
      .setRoutineType(UdfRoutineType)
      .setArguments(udf.params.unsized.map(toRoutineArgs).asJava)
      .setReturnType(udf.returnType.map(toSqlDataType).orNull)
    RoutineInfoHelper.setDescription(baseBuilder)(udf.description)
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
