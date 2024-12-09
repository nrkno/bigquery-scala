/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package client.http4s.internal

import cats.syntax.all.*
import googleapis.bigquery.*

object SchemaHelper {
  def fromTableSchema(schema: TableSchema) =
    BQSchema(fields = schema.fields.getOrElse(Nil).map(fromTableFieldSchema))

  def fromTableFieldSchema(field: TableFieldSchema): BQField =
    field match {
      case TableFieldSchema(Some(name), _, _, _, tags, desc, _, subFields, _, mode, Some(typ), _, _, _, _, _) =>
        BQField(
          name = name,
          tpe = BQField.Type.unsafeFromString(typ match {
            case "RECORD" => "STRUCT"
            case "INTEGER" => "INT64"
            case "FLOAT" => "FLOAT64"
            case x => x
          }),
          mode = mode.map(BQField.Mode.unsafeFromString).getOrElse(BQField.Mode.NULLABLE),
          subFields = subFields.getOrElse(Nil).map(fromTableFieldSchema),
          description = desc,
          policyTags = tags.flatMap(_.names).getOrElse(Nil)
        )
      case _ => sys.error(s"Unable to convert $field")
    }

  def toTableSchema(schema: BQSchema) =
    TableSchema(fields = Some(schema.fields.map(toTableFieldSchema)))

  def toTableFieldSchema(field: BQField): TableFieldSchema =
    field match {
      case BQField(name, typ, mode, desc, subFields, tags) =>
        TableFieldSchema(
          name = Some(name),
          `type` = Some(typ.name),
          mode = Some(mode.name),
          fields = if (subFields.nonEmpty) Some(subFields.map(toTableFieldSchema)) else None,
          description = desc,
          policyTags = if (tags.nonEmpty) Some(TableFieldSchemaPolicyTags(Some(tags))) else None
        )
    }

  def toBQFieldTuple(field: StandardSqlField) =
    (field.name, field.`type`).flatMapN((name, t) => toBQType(t).map(name -> _))

  def toBQType(tpe: StandardSqlDataType): Option[BQType] = {
    val kind = tpe.typeKind.get
    kind match {
      case StandardSqlDataTypeTypeKind.INT64 => Some(BQType.INT64)
      case StandardSqlDataTypeTypeKind.BOOL => Some(BQType.BOOL)
      case StandardSqlDataTypeTypeKind.FLOAT64 => Some(BQType.FLOAT64)
      case StandardSqlDataTypeTypeKind.STRING => Some(BQType.STRING)
      case StandardSqlDataTypeTypeKind.BYTES => Some(BQType.BYTES)
      case StandardSqlDataTypeTypeKind.TIMESTAMP => Some(BQType.TIMESTAMP)
      case StandardSqlDataTypeTypeKind.DATE => Some(BQType.DATE)
      case StandardSqlDataTypeTypeKind.TIME => Some(BQType.TIME)
      case StandardSqlDataTypeTypeKind.DATETIME => Some(BQType.DATETIME)
      case StandardSqlDataTypeTypeKind.INTERVAL => Some(BQType.INTERVAL)
      case StandardSqlDataTypeTypeKind.GEOGRAPHY => Some(BQType.GEOGRAPHY)
      case StandardSqlDataTypeTypeKind.NUMERIC => Some(BQType.NUMERIC)
      case StandardSqlDataTypeTypeKind.BIGNUMERIC => Some(BQType.BIGNUMERIC)
      case StandardSqlDataTypeTypeKind.JSON => Some(BQType.JSON)
      case StandardSqlDataTypeTypeKind.ARRAY =>
        tpe.arrayElementType.flatMap(toBQType).map(inner => BQType(BQField.Mode.REPEATED, inner.tpe, Nil))
      case StandardSqlDataTypeTypeKind.STRUCT =>
        tpe.structType.map(struct => BQType.struct(struct.fields.getOrElse(Nil).flatMap(toBQFieldTuple)*))
      case _ => None
    }
  }

  def fromFieldType(tpe: BQField.Type) =
    tpe match {
      case BQField.Type.BOOL => Some(StandardSqlDataTypeTypeKind.BOOL)
      case BQField.Type.INT64 => Some(StandardSqlDataTypeTypeKind.INT64)
      case BQField.Type.FLOAT64 => Some(StandardSqlDataTypeTypeKind.FLOAT64)
      case BQField.Type.NUMERIC => Some(StandardSqlDataTypeTypeKind.NUMERIC)
      case BQField.Type.BIGNUMERIC => Some(StandardSqlDataTypeTypeKind.BIGNUMERIC)
      case BQField.Type.STRING => Some(StandardSqlDataTypeTypeKind.STRING)
      case BQField.Type.BYTES => Some(StandardSqlDataTypeTypeKind.BYTES)
      case BQField.Type.TIMESTAMP => Some(StandardSqlDataTypeTypeKind.TIMESTAMP)
      case BQField.Type.DATE => Some(StandardSqlDataTypeTypeKind.DATE)
      case BQField.Type.TIME => Some(StandardSqlDataTypeTypeKind.TIME)
      case BQField.Type.DATETIME => Some(StandardSqlDataTypeTypeKind.DATETIME)
      case BQField.Type.GEOGRAPHY => Some(StandardSqlDataTypeTypeKind.GEOGRAPHY)
      case BQField.Type.JSON => Some(StandardSqlDataTypeTypeKind.JSON)
      case BQField.Type.INTERVAL => Some(StandardSqlDataTypeTypeKind.INTERVAL)
      case BQField.Type.STRUCT => None
      case BQField.Type.ARRAY => None
      case BQField.Type.RANGE => None
    }

  def fromBQType(_tpe: BQType): StandardSqlDataType =
    _tpe match {
      case BQType(BQField.Mode.REPEATED, tpe, subFields) =>
        StandardSqlDataType(
          arrayElementType = Some(fromBQType(BQType(BQField.Mode.NULLABLE, tpe, subFields))),
          typeKind = Some(StandardSqlDataTypeTypeKind.ARRAY))
      case BQType(_, BQField.Type.STRUCT, subFields) =>
        StandardSqlDataType(
          structType = Some(StandardSqlStructType(Some(subFields.map { case (name, typ) =>
            StandardSqlField(Some(name), Some(fromBQType(typ)))
          }))),
          typeKind = Some(StandardSqlDataTypeTypeKind.STRUCT)
        )
      case BQType(_, typ, _) =>
        StandardSqlDataType(typeKind = fromFieldType(typ))
    }

  def fromTableType(tpe: StandardSqlTableType) = {
    val cols = tpe.columns.getOrElse(Nil)
    BQType.struct(cols.flatMap(toBQFieldTuple)*).asSchema.toOption
  }

  def toTableType(schema: BQSchema): StandardSqlTableType =
    StandardSqlTableType(
      Some(schema.fields.map(f => StandardSqlField(Some(f.name), Some(fromBQType(BQType.fromField(f)))))))

}
