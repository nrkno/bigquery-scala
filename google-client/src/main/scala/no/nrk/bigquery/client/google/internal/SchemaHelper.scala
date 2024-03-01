/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package client.google.internal

import com.google.cloud.bigquery.{Option as _, *}
import no.nrk.bigquery.BQField.Type

import scala.jdk.CollectionConverters.*

object SchemaHelper {
  def fromType(typ: Type): StandardSQLTypeName = StandardSQLTypeName.valueOf(typ.name)
  def fromMode(mode: BQField.Mode): Field.Mode = Field.Mode.valueOf(mode.name)

  def toSchema(schema: BQSchema): Schema =
    Schema.of(schema.fields.map(toField)*)

  def typeFrom(dt: StandardSQLDataType): Option[BQType] =
    for {
      typ <- BQField.Type.fromString(dt.getTypeKind)
      arr = Option(dt.getArrayElementType).flatMap(e => typeFrom(e))
      struct = Option(dt.getStructType).map(e =>
        e.getFields.asScala.flatMap(f => typeFrom(f.getDataType).map(t => f.getName -> t)).toList)
    } yield BQType(
      if (arr.isDefined) BQField.Mode.REPEATED else BQField.Mode.NULLABLE,
      arr.map(_.tpe).getOrElse(typ),
      struct.orElse(arr.map(_.subFields)).getOrElse(Nil)
    )

  def fromSchema(schema: Schema): BQSchema =
    schema match {
      case null => BQSchema(Nil)
      case schema =>
        BQSchema(schema.getFields.asScala.toList.map(fromField))
    }

  def toField(field: BQField): Field = {
    val b = Field.newBuilder(field.name, fromType(field.tpe), field.subFields.map(toField)*)
    b.setMode(fromMode(field.mode))
    field.description.foreach(b.setDescription)
    if (field.policyTags.nonEmpty) {
      b.setPolicyTags(
        PolicyTags.newBuilder().setNames(field.policyTags.asJava).build()
      )
      ()
    }
    b.build()
  }

  def fromField(f: Field): BQField =
    BQField(
      name = f.getName,
      mode = mapMode(f.getMode),
      tpe = BQField.Type.unsafeFromString(f.getType.getStandardType.name()),
      description = mapDescription(f.getDescription),
      subFields = mapSubFields(f.getSubFields),
      policyTags = Option(f.getPolicyTags)
        .flatMap(pt => Option(pt.getNames))
        .fold(List.empty[String])(_.asScala.toList)
    )

  def mapMode(mode: Field.Mode): BQField.Mode =
    BQField.Mode.unsafeFromString((mode match {
      case null => Field.Mode.NULLABLE
      case other => other
    }).name())

  def mapDescription(description: String): Option[String] =
    description match {
      case null => None
      case "" => None
      case other => Some(other)
    }

  def mapSubFields(fs: FieldList): List[BQField] =
    Option(fs) match {
      case Some(fs) => fs.asScala.toList.map(fromField)
      case None => List.empty
    }

  def bqTypeFromSchema(schema: Schema): BQType =
    BQType.fromBQSchema(fromSchema(schema))
}
