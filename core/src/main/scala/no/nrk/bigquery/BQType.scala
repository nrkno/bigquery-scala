/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

/** This is the schema of a BQ type, where fields don't have names until they are placed inside a struct.
  *
  * We also use it for query responses, where single-column corresponds to a non-struct `BQType`, and otherwise all
  * chosen columns are packed into a struct
  */
case class BQType(
    mode: BQField.Mode,
    tpe: BQField.Type,
    subFields: List[(String, BQType)]
) {
  def nullable: BQType = copy(mode = BQField.Mode.NULLABLE)
  def repeated: BQType = copy(mode = BQField.Mode.REPEATED)

  def asSchema: Either[String, BQSchema] = {
    def go(name: String, tpe: BQType): BQField =
      BQField(
        name,
        tpe.tpe,
        tpe.mode,
        subFields = tpe.subFields.map { case (name, tpe) => go(name, tpe) }
      )

    tpe match {
      case BQField.Type.STRUCT =>
        val fields = subFields.map { case (name, tpe) => go(name, tpe) }
        Right(BQSchema(fields))
      case other => Left(s"must be struct at top-level, not $other")
    }
  }
}

object BQType {
  val BOOL: BQType = apply(BQField.Mode.REQUIRED, BQField.Type.BOOL, Nil)
  val INT64: BQType = apply(BQField.Mode.REQUIRED, BQField.Type.INT64, Nil)
  val FLOAT64: BQType =
    apply(BQField.Mode.REQUIRED, BQField.Type.FLOAT64, Nil)
  val NUMERIC: BQType =
    apply(BQField.Mode.REQUIRED, BQField.Type.NUMERIC, Nil)
  val BIGNUMERIC: BQType =
    apply(BQField.Mode.REQUIRED, BQField.Type.BIGNUMERIC, Nil)
  val STRING: BQType =
    apply(BQField.Mode.REQUIRED, BQField.Type.STRING, Nil)
  val BYTES: BQType = apply(BQField.Mode.REQUIRED, BQField.Type.BYTES, Nil)
  val TIMESTAMP: BQType =
    apply(BQField.Mode.REQUIRED, BQField.Type.TIMESTAMP, Nil)
  val DATE: BQType = apply(BQField.Mode.REQUIRED, BQField.Type.DATE, Nil)
  val TIME: BQType = apply(BQField.Mode.REQUIRED, BQField.Type.TIME, Nil)
  val DATETIME: BQType =
    apply(BQField.Mode.REQUIRED, BQField.Type.DATETIME, Nil)
  val GEOGRAPHY: BQType =
    apply(BQField.Mode.REQUIRED, BQField.Type.GEOGRAPHY, Nil)

  def struct(subFields: (String, BQType)*): BQType =
    BQType(BQField.Mode.REQUIRED, BQField.Type.STRUCT, subFields.toList)

  val StringDictionary: BQType =
    BQType.struct("index" -> BQType.INT64, "value" -> BQType.STRING).repeated

  val NumberDictionary: BQType =
    BQType.struct("index" -> BQType.INT64, "value" -> BQType.INT64).repeated

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
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          more.map(f => f.name -> fromField(f))
        )
    }

  def format(f: BQType): String =
    f match {
      case f if f.tpe == BQField.Type.STRUCT && f.mode == BQField.Mode.REPEATED =>
        s"ARRAY<STRUCT<${f.subFields
            .map { case (name, sf) => s"$name ${format(sf)}" }
            .mkString(", ")}>>"
      case f if f.tpe == BQField.Type.STRUCT =>
        s"STRUCT<${f.subFields.map { case (name, sf) => s"$name ${format(sf)}" }.mkString(", ")}>"
      case f if f.tpe == BQField.Type.ARRAY =>
        s"ARRAY(${format(f.subFields.head._2)})"
      case other if f.mode == BQField.Mode.REPEATED =>
        s"ARRAY<${other.tpe.name}>"
      case other =>
        s"${other.tpe.name}"
    }
}
