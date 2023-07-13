package no.nrk.bigquery.internal

import com.google.cloud.bigquery.{Field, FieldList, PolicyTags, Schema, StandardSQLTypeName}

import scala.jdk.CollectionConverters._
import no.nrk.bigquery.{BQField, BQSchema, BQType}

object SchemaHelper {
  def fromType(typ: BQField.Type): StandardSQLTypeName = StandardSQLTypeName.valueOf(typ.name)
  def fromMode(mode: BQField.Mode): Field.Mode = Field.Mode.valueOf(mode.name)

  def toSchema(schema: BQSchema): Schema =
    Schema.of(schema.fields.map(toField): _*)

  def fromSchema(schema: Schema): BQSchema =
    schema match {
      case null => BQSchema(Nil)
      case schema =>
        BQSchema(schema.getFields.asScala.toList.map(fromField))
    }

  def toField(field: BQField): Field = {
    val b = Field.newBuilder(field.name, fromType(field.tpe), field.subFields.map(toField): _*)
    b.setMode(fromMode(field.mode))
    field.description.foreach(b.setDescription)
    if (field.policyTags.nonEmpty)
      b.setPolicyTags(
        PolicyTags.newBuilder().setNames(field.policyTags.asJava).build()
      )
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
