/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

/** This is isomorphic to `Field` (can translate back and forth without data lass)
  *
  * We need this because
  *   - it avoids quite a few inconsistencies (see `BQField.setDescription` and `BQField.mapMode`)
  *   - for that reason it can more safely be compared, including with descriptions and modes. If you want to compare it
  *     without those, see `BQType` and `BQTypeOutline`
  *   - it has nicer syntax to construct
  *   - it is also way easier to write transform and write recursive code on top of
  */
case class BQField(
    name: String,
    tpe: BQField.Type,
    mode: BQField.Mode,
    description: Option[String] = None,
    subFields: List[BQField] = Nil,
    policyTags: List[String] = Nil
) {
  val ident: Ident = Ident(name)

  /** see description in [[BQSchema.recursivelyNullable]] */
  def recursivelyNullable: BQField =
    copy(
      mode = if (isRequired) BQField.Mode.NULLABLE else mode,
      subFields = subFields.map(_.recursivelyNullable)
    )

  def isRequired: Boolean = mode == BQField.Mode.REQUIRED

  def withName(newName: String) = copy(name = newName)

  def mapName(f: String => String) = withName(f(name))

  def withDescription(desc: String) = copy(description = Some(desc))
  def withoutDescription = copy(description = None)

  def withRequired = copy(mode = BQField.Mode.REQUIRED)
  def withRepeated = copy(mode = BQField.Mode.REPEATED)

  def withType(newType: BQField.Type) = copy(tpe = newType)

  def addSubFields(newSubFields: BQField*) =
    withSubFields(subFields ::: newSubFields.toList)
  def withSubFields(newSubFields: List[BQField]) =
    if (tpe == BQField.Type.STRUCT) copy(subFields = newSubFields) else this

  def addPolicyTags(newTags: String*) = copy(policyTags = policyTags ::: newTags.toList)
  def withPolicyTags(newTags: List[String]) = copy(policyTags = newTags)
}

object BQField {
  sealed abstract class Type(val name: String) extends Product with Serializable {
    override def toString: String = name

    override def equals(obj: Any): Boolean =
      obj match {
        case m: Type => m.name == name
        case _ => false
      }

    override def hashCode(): Int = 31 * name.hashCode
  }
  object Type {

    /** A Boolean value (true or false). */
    case object BOOL extends Type("BOOL")

    /** A 64-bit signed integer value. */
    case object INT64 extends Type("INT64")

    /** A 64-bit IEEE binary floating-point value. */
    case object FLOAT64 extends Type("FLOAT64")

    /** A decimal value with 38 digits of precision and 9 digits of scale. */
    case object NUMERIC extends Type("NUMERIC")

    /** A decimal value with 76+ digits of precision (the 77th digit is partial) and 38 digits of scale
      */
    case object BIGNUMERIC extends Type("BIGNUMERIC")

    /** Variable-length character (Unicode) data. */
    case object STRING extends Type("STRING")

    /** Variable-length binary data. */
    case object BYTES extends Type("BYTES")

    /** Container of ordered fields each with a type (required) and field name (optional). */
    case object STRUCT extends Type("STRUCT")

    /** Ordered list of zero or more elements of any non-array type. */
    case object ARRAY extends Type("ARRAY")

    /** Represents an absolute point in time, with microsecond precision. Values range between the years 1 and 9999,
      * inclusive.
      */
    case object TIMESTAMP extends Type("TIMESTAMP")

    /** Represents a logical calendar date. Values range between the years 1 and 9999, inclusive. */
    case object DATE extends Type("DATE")

    /** Represents a time, independent of a specific date, to microsecond precision. */
    case object TIME extends Type("TIME")

    /** Represents a year, month, day, hour, minute, second, and subsecond (microsecond precision). */
    case object DATETIME extends Type("DATETIME")

    /** Represents a set of geographic points, represented as a Well Known Text (WKT) string. */
    case object GEOGRAPHY extends Type("GEOGRAPHY")

    /** Represents JSON data. */
    case object JSON extends Type("JSON")

    /** Represents duration or amount of time. */
    case object INTERVAL extends Type("INTERVAL")

    val values: List[Type] = List(
      BOOL,
      INT64,
      FLOAT64,
      NUMERIC,
      BIGNUMERIC,
      STRING,
      BYTES,
      STRUCT,
      ARRAY,
      TIMESTAMP,
      DATE,
      TIME,
      DATETIME,
      GEOGRAPHY,
      JSON,
      INTERVAL)

    def fromString(name: String): Option[Type] = values.find(_.name == name.toUpperCase)

    def unsafeFromString(name: String): Type =
      fromString(name).getOrElse(throw new NoSuchElementException(s"$name is not a valid Type"))
  }

  sealed abstract class Mode(val name: String) extends Product with Serializable {
    override def toString: String = name

    override def equals(obj: Any): Boolean =
      obj match {
        case m: Mode => m.name == name
        case _ => false
      }

    override def hashCode(): Int = 31 * name.hashCode
  }
  object Mode {
    case object REQUIRED extends Mode("REQUIRED")
    case object NULLABLE extends Mode("NULLABLE")
    case object REPEATED extends Mode("REPEATED")

    val values: List[Mode] = List(REQUIRED, NULLABLE, REPEATED)

    def fromString(name: String): Option[Mode] = values.find(_.name == name.toUpperCase)

    def unsafeFromString(name: String): Mode =
      fromString(name).getOrElse(throw new NoSuchElementException(s"$name is not a valid Mode"))
  }

  // convenience constructor for structs
  def struct(
      name: String,
      mode: BQField.Mode,
      description: Option[String] = None
  )(subFields: BQField*): BQField =
    BQField(
      name,
      BQField.Type.STRUCT,
      mode,
      description,
      subFields.toList,
      Nil
    )

  // convenience constructor for arrays
  def repeated(
      name: String,
      tpe: BQField.Type,
      description: Option[String] = None
  ): BQField =
    BQField(name, tpe, BQField.Mode.REPEATED, description, Nil, Nil)

  // convenience constructor for repeated structs
  def repeatedStruct(name: String, description: Option[String] = None)(
      subFields: BQField*
  ): BQField =
    BQField(
      name,
      BQField.Type.STRUCT,
      BQField.Mode.REPEATED,
      description,
      subFields.toList,
      Nil
    )
}
