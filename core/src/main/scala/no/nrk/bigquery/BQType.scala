package no.nrk.bigquery

import com.google.cloud.bigquery.{Field, Schema, StandardSQLTypeName}
import io.circe.{Decoder, Encoder}

import scala.util.Try

/** This is the schema of a BQ type, where fields don't have names until they are placed inside a struct.
  *
  * We also use it for query responses, where single-column corresponds to a non-struct `BQType`, and otherwise all
  * chosen columns are packed into a struct
  */
case class BQType(
    mode: Field.Mode,
    tpe: StandardSQLTypeName,
    subFields: List[(String, BQType)]
) {
  def nullable: BQType = copy(mode = Field.Mode.NULLABLE)
  def repeated: BQType = copy(mode = Field.Mode.REPEATED)

  def asSchema: Either[String, BQSchema] = {
    def go(name: String, tpe: BQType): BQField =
      BQField(
        name,
        tpe.tpe,
        tpe.mode,
        subFields = tpe.subFields.map { case (name, tpe) => go(name, tpe) }
      )

    tpe match {
      case StandardSQLTypeName.STRUCT =>
        val fields = subFields.map { case (name, tpe) => go(name, tpe) }
        Right(BQSchema(fields))
      case other => Left(s"must be struct at top-level, not $other")
    }
  }
}

object BQType {
  val BOOL: BQType = apply(Field.Mode.REQUIRED, StandardSQLTypeName.BOOL, Nil)
  val INT64: BQType = apply(Field.Mode.REQUIRED, StandardSQLTypeName.INT64, Nil)
  val FLOAT64: BQType =
    apply(Field.Mode.REQUIRED, StandardSQLTypeName.FLOAT64, Nil)
  val NUMERIC: BQType =
    apply(Field.Mode.REQUIRED, StandardSQLTypeName.NUMERIC, Nil)
  val BIGNUMERIC: BQType =
    apply(Field.Mode.REQUIRED, StandardSQLTypeName.BIGNUMERIC, Nil)
  val STRING: BQType =
    apply(Field.Mode.REQUIRED, StandardSQLTypeName.STRING, Nil)
  val BYTES: BQType = apply(Field.Mode.REQUIRED, StandardSQLTypeName.BYTES, Nil)
  val TIMESTAMP: BQType =
    apply(Field.Mode.REQUIRED, StandardSQLTypeName.TIMESTAMP, Nil)
  val DATE: BQType = apply(Field.Mode.REQUIRED, StandardSQLTypeName.DATE, Nil)
  val TIME: BQType = apply(Field.Mode.REQUIRED, StandardSQLTypeName.TIME, Nil)
  val DATETIME: BQType =
    apply(Field.Mode.REQUIRED, StandardSQLTypeName.DATETIME, Nil)
  val GEOGRAPHY: BQType =
    apply(Field.Mode.REQUIRED, StandardSQLTypeName.GEOGRAPHY, Nil)

  def struct(subFields: (String, BQType)*): BQType =
    BQType(Field.Mode.REQUIRED, StandardSQLTypeName.STRUCT, subFields.toList)

  val StringDictionary: BQType =
    BQType.struct("index" -> BQType.INT64, "value" -> BQType.STRING).repeated

  val NumberDictionary: BQType =
    BQType.struct("index" -> BQType.INT64, "value" -> BQType.INT64).repeated

  def fromSchema(schema: Schema): BQType =
    fromBQSchema(BQSchema.fromSchema(schema))

  def fromField(field: BQField): BQType =
    BQType(
      field.mode,
      field.tpe,
      field.subFields.map(f => f.name -> fromField(f))
    )

  def fromBQSchema(schema: BQSchema): BQType =
    schema.fields match {
      case List(one) => fromField(one)
      case more =>
        BQType(
          Field.Mode.REQUIRED,
          StandardSQLTypeName.STRUCT,
          more.map(f => f.name -> fromField(f))
        )
    }

  def format(f: BQType): String =
    f match {
      case f if f.tpe == StandardSQLTypeName.STRUCT && f.mode == Field.Mode.REPEATED =>
        s"ARRAY<STRUCT<${f.subFields
            .map { case (name, sf) => s"$name ${format(sf)}" }
            .mkString(", ")}>>"
      case f if f.tpe == StandardSQLTypeName.STRUCT =>
        s"STRUCT<${f.subFields.map { case (name, sf) => s"$name ${format(sf)}" }.mkString(", ")}>"
      case f if f.tpe == StandardSQLTypeName.ARRAY =>
        s"ARRAY(${format(f.subFields.head._2)})"
      case other if f.mode == Field.Mode.REPEATED =>
        s"ARRAY<${other.tpe.name}>"
      case other =>
        s"${other.tpe.name}"
    }

  implicit lazy val encodesLegacySQLTypeName: Encoder[StandardSQLTypeName] =
    Encoder.encodeString.contramap(_.name)
  implicit lazy val encodesFieldMode: Encoder[Field.Mode] =
    Encoder.encodeString.contramap(_.name)
  implicit lazy val encodes: Encoder[BQType] =
    io.circe.generic.semiauto.deriveEncoder

  implicit lazy val decodesStandardSQLTypeName: Decoder[StandardSQLTypeName] =
    Decoder.decodeString.emapTry(str => Try(StandardSQLTypeName.valueOf(str)))
  implicit lazy val decodesFieldMode: Decoder[Field.Mode] =
    Decoder.decodeString.emapTry(str => Try(Field.Mode.valueOf(str)))
  implicit lazy val decodes: Decoder[BQType] =
    io.circe.generic.semiauto.deriveDecoder
}
