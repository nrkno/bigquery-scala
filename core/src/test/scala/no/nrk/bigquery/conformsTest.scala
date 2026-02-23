/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import munit.FunSuite

class conformsTest extends FunSuite {
  test("failing conform by type position") {
    val violations = conforms.onlyTypes(
      BQSchema.of(
        BQField("one", BQField.Type.STRING, BQField.Mode.NULLABLE),
        BQField("two", BQField.Type.INT64, BQField.Mode.NULLABLE)
      ),
      BQSchema.of(
        BQField("one", BQField.Type.INT64, BQField.Mode.NULLABLE),
        BQField("two", BQField.Type.STRING, BQField.Mode.NULLABLE)
      )
    )
    assertEquals(violations.map(_.length), Some(2), clue(violations.map(_.mkString(", "))))
  }

  test("passing conform by type position") {
    val violations = conforms.onlyTypes(
      BQSchema.of(
        BQField("one", BQField.Type.STRING, BQField.Mode.NULLABLE),
        BQField("two", BQField.Type.INT64, BQField.Mode.NULLABLE)
      ),
      BQSchema.of(
        BQField("one", BQField.Type.STRING, BQField.Mode.NULLABLE),
        BQField("two", BQField.Type.INT64, BQField.Mode.NULLABLE)
      )
    )
    assertEquals(violations.map(_.length), None, clue(violations.map(_.mkString(", "))))
  }

  test("passing conform by name and type") {
    val violations = conforms.typesAndName(
      BQSchema.of(
        BQField("one", BQField.Type.STRING, BQField.Mode.NULLABLE),
        BQField("two", BQField.Type.INT64, BQField.Mode.NULLABLE)
      ),
      BQSchema.of(
        BQField("two", BQField.Type.INT64, BQField.Mode.NULLABLE),
        BQField("one", BQField.Type.STRING, BQField.Mode.NULLABLE)
      )
    )
    assertEquals(violations.map(_.length), None, clue(violations.map(_.mkString(", "))))
  }

  test("failing conform by name and type") {
    val violations = conforms.typesAndName(
      BQSchema.of(
        BQField("one", BQField.Type.STRING, BQField.Mode.NULLABLE),
        BQField("two", BQField.Type.INT64, BQField.Mode.NULLABLE)
      ),
      BQSchema.of(
        BQField("two", BQField.Type.STRING, BQField.Mode.NULLABLE),
        BQField("one", BQField.Type.INT64, BQField.Mode.NULLABLE)
      )
    )
    assertEquals(violations.map(_.length), Some(2), clue(violations.map(_.mkString(", "))))
  }

  test("fieldCounts detects when actual has fewer fields") {
    val violations = conforms.fieldCounts(
      BQSchema.of(
        BQField("one", BQField.Type.STRING, BQField.Mode.NULLABLE)
      ),
      BQSchema.of(
        BQField("one", BQField.Type.STRING, BQField.Mode.NULLABLE),
        BQField("two", BQField.Type.INT64, BQField.Mode.NULLABLE)
      )
    )
    assert(violations.isDefined, clue(violations))
    assert(violations.get.head.contains("expected 2 fields, got 1"), clue(violations))
  }

  test("fieldCounts detects when actual has more fields") {
    val violations = conforms.fieldCounts(
      BQSchema.of(
        BQField("one", BQField.Type.STRING, BQField.Mode.NULLABLE),
        BQField("two", BQField.Type.INT64, BQField.Mode.NULLABLE),
        BQField("three", BQField.Type.BOOL, BQField.Mode.NULLABLE)
      ),
      BQSchema.of(
        BQField("one", BQField.Type.STRING, BQField.Mode.NULLABLE),
        BQField("two", BQField.Type.INT64, BQField.Mode.NULLABLE)
      )
    )
    assert(violations.isDefined, clue(violations))
    assert(violations.get.head.contains("expected 2 fields, got 3"), clue(violations))
  }

  test("fieldCounts passes when counts match") {
    val schema = BQSchema.of(
      BQField("one", BQField.Type.STRING, BQField.Mode.NULLABLE),
      BQField("two", BQField.Type.INT64, BQField.Mode.NULLABLE)
    )
    val violations = conforms.fieldCounts(schema, schema)
    assertEquals(violations, None, clue(violations))
  }

  test("fieldCounts checks nested struct field counts") {
    val actualSchema = BQSchema.of(
      BQField(
        "outer",
        BQField.Type.STRUCT,
        BQField.Mode.NULLABLE,
        subFields = List(
          BQField("inner1", BQField.Type.STRING, BQField.Mode.NULLABLE)
        )
      )
    )
    val givenSchema = BQSchema.of(
      BQField(
        "outer",
        BQField.Type.STRUCT,
        BQField.Mode.NULLABLE,
        subFields = List(
          BQField("inner1", BQField.Type.STRING, BQField.Mode.NULLABLE),
          BQField("inner2", BQField.Type.INT64, BQField.Mode.NULLABLE)
        )
      )
    )
    val violations = conforms.fieldCounts(actualSchema, givenSchema)
    assert(violations.isDefined, clue(violations))
    assert(violations.get.head.contains("outer"), clue(violations))
    assert(violations.get.head.contains("expected 2 fields, got 1"), clue(violations))
  }

}
