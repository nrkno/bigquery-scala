package no.nrk.bigquery

import com.google.cloud.bigquery.{Field, FieldList, PolicyTags, StandardSQLTypeName}
import io.circe.{Decoder, Encoder}

import scala.jdk.CollectionConverters._

/** This is isomorphic to `Field` (can translate back and forth without data lass)
  *
  * We need this because
  *   - it avoids quite a few inconsistencies (see `BQField.setDescription` and `BQField.mapMode`)
  *   - for that reason it can more safely be compared, including with descriptions and modes. If you want to compare it without those, see `BQType` and
  *     `BQTypeOutline`
  *   - it has nicer syntax to construct
  *   - it is also way easier to write transform and write recursive code on top of
  */
case class BQField(
    name: String,
    tpe: StandardSQLTypeName,
    mode: Field.Mode,
    description: Option[String] = None,
    subFields: List[BQField] = Nil,
    policyTags: List[String] = Nil
) {
  val ident: Ident = Ident(name)

  def toField: Field = {
    val b = Field.newBuilder(name, tpe, subFields.map(_.toField): _*)
    b.setMode(mode)
    description.foreach(b.setDescription)
    if (policyTags.nonEmpty) b.setPolicyTags(PolicyTags.newBuilder().setNames(policyTags.asJava).build())
    b.build()
  }

  /** see description in [[BQSchema.recursivelyNullable]] */
  def recursivelyNullable: BQField =
    copy(
      mode = if (mode == Field.Mode.REQUIRED) Field.Mode.NULLABLE else mode,
      subFields = subFields.map(_.recursivelyNullable)
    )
}

object BQField {
  implicit lazy val encodes: Encoder[BQField] = {
    implicit val encodesStandardSQLTypeName: Encoder[StandardSQLTypeName] = Encoder[String].contramap(_.name())
    implicit val encodesFieldMode: Encoder[Field.Mode] = Encoder[String].contramap(_.name())
    assertIsUsed(encodesStandardSQLTypeName, encodesFieldMode)
    io.circe.generic.semiauto.deriveEncoder
  }

  implicit lazy val decodes: Decoder[BQField] = {
    implicit val decodesStandardSQLTypeName: Decoder[StandardSQLTypeName] = Decoder[String].map(StandardSQLTypeName.valueOf)
    implicit val decodesFieldMode: Decoder[Field.Mode] = Decoder[String].map(Field.Mode.valueOf)
    assertIsUsed(decodesStandardSQLTypeName, decodesFieldMode)
    io.circe.generic.semiauto.deriveDecoder
  }

  // convenience constructor for structs
  def struct(name: String, mode: Field.Mode, description: Option[String] = None)(subFields: BQField*): BQField =
    BQField(name, StandardSQLTypeName.STRUCT, mode, description, subFields.toList, Nil)

  // convenience constructor for arrays
  def repeated(name: String, tpe: StandardSQLTypeName, description: Option[String] = None): BQField =
    BQField(name, tpe, Field.Mode.REPEATED, description, Nil, Nil)

  // convenience constructor for repeated structs
  def repeatedStruct(name: String, description: Option[String] = None)(subFields: BQField*): BQField =
    BQField(name, StandardSQLTypeName.STRUCT, Field.Mode.REPEATED, description, subFields.toList, Nil)

  def fromField(f: Field): BQField =
    BQField(
      name = f.getName,
      mode = mapMode(f.getMode),
      tpe = f.getType.getStandardType,
      description = mapDescription(f.getDescription),
      subFields = mapSubFields(f.getSubFields),
      policyTags = Option(f.getPolicyTags).flatMap(pt => Option(pt.getNames)).fold(List.empty[String])(_.asScala.toList)
    )

  def mapMode(mode: Field.Mode): Field.Mode =
    mode match {
      case null  => Field.Mode.NULLABLE
      case other => other
    }

  def mapDescription(description: String): Option[String] =
    description match {
      case null  => None
      case ""    => None
      case other => Some(other)
    }

  def mapSubFields(fs: FieldList): List[BQField] =
    Option(fs) match {
      case Some(fs) => fs.asScala.toList.map(fromField)
      case None     => List.empty
    }
}
