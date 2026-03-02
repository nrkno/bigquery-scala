/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

/** conformance checks for BigQuery queries, schemas and types.
  *
  * All methods return None if schemas conform, or Some(reasons) listing mismatches.
  */
object conforms {

  // Positional check of types/modes only - anonymizes all field names before comparing
  def onlyTypes(
      actualSchema: BQSchema,
      givenType: BQType
  ): Option[List[String]] = {

    val Anon = "_"
    def asAnonField(bqType: BQType): BQField =
      BQField(
        Anon,
        bqType.tpe,
        bqType.mode,
        None,
        bqType.subFields.map { case (_, tpe) => asAnonField(tpe) },
        Nil
      )
    def anonymize(field: BQField): BQField =
      field.copy(name = Anon, subFields = field.subFields.map(anonymize))

    val givenSchema = asAnonField(givenType) match {
      case BQField(_, BQField.Type.STRUCT, _, _, subFields, Nil) =>
        BQSchema(subFields.toList)
      case other => BQSchema.of(other)
    }

    onlyTypes(
      actualSchema.copy(fields = actualSchema.fields.map(anonymize)),
      givenSchema
    )
  }

  // Positional check: compares fields by index, checking name, type and mode (REPEATED vs not)
  def onlyTypes(
      actualSchema: BQSchema,
      givenSchema: BQSchema
  ): Option[List[String]] = {
    val reasonsBuilder = List.newBuilder[String]

    def go(
        path: List[BQField],
        actualFields: Seq[BQField],
        givenFields: Seq[BQField]
    ): Unit =
      actualFields.zipWithIndex.foreach { case (actualField, idx) =>
        val givenFieldOpt = givenFields.lift(idx)

        // if we're inside structs, render the full path
        def render(f: BQField) =
          s"field `${(f :: path).reverse.map(_.name).mkString(".")}`"

        givenFieldOpt match {
          case Some(givenField) if givenField.name != actualField.name =>
            reasonsBuilder += s"Expected ${render(actualField)}, got ${render(givenField)}"
          case Some(givenField) if givenField.tpe != actualField.tpe =>
            reasonsBuilder += s"Expected ${render(actualField)} to have type ${actualField.tpe}, got ${givenField.tpe}"
          case Some(givenField)
              if (givenField.mode == BQField.Mode.REPEATED) != (actualField.mode == BQField.Mode.REPEATED) =>
            reasonsBuilder += s"Expected ${render(actualField)} to have mode ${actualField.mode}, got ${givenField.mode}"
          case Some(givenField) if givenField.subFields.nonEmpty =>
            go(givenField :: path, actualField.subFields, givenField.subFields)
          case Some(ok @ _) =>
            ()
          case None =>
            reasonsBuilder += s"Expected ${render(actualField)} at 0-based index $idx, but it given table/struct was shorter"
        }
      }

    go(Nil, actualSchema.fields, givenSchema.fields)

    reasonsBuilder.result() match {
      case Nil => None
      case reasons => Some(reasons)
    }
  }

  // Converts BQType to schema preserving names, then does name-based comparison via typesAndName
  def types(
      actualSchema: BQSchema,
      givenType: BQType
  ): Option[List[String]] = {

    def asField(name: String, bqType: BQType): BQField =
      BQField(
        name,
        bqType.tpe,
        bqType.mode,
        None,
        bqType.subFields.map { case (subName, tpe) => asField(subName, tpe) },
        Nil
      )

    // the _ can cause issues when we only have one type!
    val givenSchema = asField("_", givenType) match {
      case BQField(_, BQField.Type.STRUCT, _, _, subFields, Nil) =>
        BQSchema(subFields)
      case other => BQSchema.of(other)
    }

    typesAndName(actualSchema, givenSchema)
  }

  // Name-based check: looks up fields by name, then compares type and mode
  def typesAndName(
      actualSchema: BQSchema,
      givenSchema: BQSchema
  ): Option[List[String]] = {
    val reasonsBuilder = List.newBuilder[String]

    def go(
        path: List[BQField],
        actualFields: Seq[BQField],
        givenFields: Seq[BQField]
    ): Unit =
      actualFields.foreach { actualField =>
        val givenFieldOpt = givenFields.find(_.name == actualField.name)

        // if we're inside structs, render the full path
        def render(f: BQField) =
          s"field `${(f :: path).reverse.map(_.name).mkString(".")}`"

        givenFieldOpt match {
          case Some(givenField) if givenField.tpe != actualField.tpe =>
            reasonsBuilder += s"Expected ${render(actualField)} to have type ${actualField.tpe}, got ${givenField.tpe}"
          case Some(givenField)
              if (givenField.mode == BQField.Mode.REPEATED) != (actualField.mode == BQField.Mode.REPEATED) =>
            reasonsBuilder += s"Expected ${render(actualField)} to have mode ${actualField.mode}, got ${givenField.mode}"
          case Some(givenField) if givenField.subFields.nonEmpty =>
            go(givenField :: path, actualField.subFields, givenField.subFields)
          case Some(ok @ _) =>
            ()
          case None =>
            reasonsBuilder += s"Expected ${render(actualField)} to part of [${givenFields.map(_.name).mkString(", ")}]"
        }
      }

    go(Nil, actualSchema.fields, givenSchema.fields)

    reasonsBuilder.result() match {
      case Nil => None
      case reasons => Some(reasons)
    }
  }

  // Checks that field counts match at each nesting level (root and nested structs)
  def fieldCounts(
      actualSchema: BQSchema,
      givenSchema: BQSchema
  ): Option[List[String]] = {
    val reasonsBuilder = List.newBuilder[String]

    def go(
        path: List[String],
        actualFields: Seq[BQField],
        givenFields: Seq[BQField]
    ): Unit = {
      val pathStr = if (path.isEmpty) "root" else path.reverse.mkString(".")

      if (actualFields.size != givenFields.size) {
        reasonsBuilder += s"At $pathStr: expected ${givenFields.size} fields, got ${actualFields.size}. " +
          s"Expected: [${givenFields.map(_.name).mkString(", ")}], " +
          s"got: [${actualFields.map(_.name).mkString(", ")}]"
      }

      // Recursively check struct subfields by matching on position
      actualFields.zip(givenFields).foreach { case (actualField, givenField) =>
        if (actualField.tpe == BQField.Type.STRUCT && actualField.subFields.nonEmpty) {
          go(actualField.name :: path, actualField.subFields, givenField.subFields)
        }
      }
    }

    go(Nil, actualSchema.fields, givenSchema.fields)

    reasonsBuilder.result() match {
      case Nil => None
      case reasons => Some(reasons)
    }
  }

  // Overload: converts BQType to BQSchema, then checks field counts
  def fieldCounts(
      actualSchema: BQSchema,
      givenType: BQType
  ): Option[List[String]] = {
    def asField(name: String, bqType: BQType): BQField =
      BQField(
        name,
        bqType.tpe,
        bqType.mode,
        None,
        bqType.subFields.map { case (subName, tpe) => asField(subName, tpe) },
        Nil
      )

    val givenSchema = asField("_", givenType) match {
      case BQField(_, BQField.Type.STRUCT, _, _, subFields, Nil) =>
        BQSchema(subFields)
      case other => BQSchema.of(other)
    }

    fieldCounts(actualSchema, givenSchema)
  }
}
