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

  test("onlyTypes detects field count mismatch - fewer given fields") {
    val violations = conforms.onlyTypes(
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
    assert(violations.isDefined, "Expected violations for field count mismatch")
    assert(
      violations.get.exists(_.contains("Field count mismatch")),
      clue(violations.map(_.mkString(", ")))
    )
  }

  test("onlyTypes detects field count mismatch - more given fields") {
    val violations = conforms.onlyTypes(
      BQSchema.of(
        BQField("one", BQField.Type.STRING, BQField.Mode.NULLABLE)
      ),
      BQSchema.of(
        BQField("one", BQField.Type.STRING, BQField.Mode.NULLABLE),
        BQField("two", BQField.Type.INT64, BQField.Mode.NULLABLE)
      )
    )
    assert(violations.isDefined, "Expected violations for field count mismatch")
    assert(
      violations.get.exists(_.contains("Field count mismatch")),
      clue(violations.map(_.mkString(", ")))
    )
  }

  test("typesAndName detects field count mismatch") {
    val violations = conforms.typesAndName(
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
    assert(violations.isDefined, "Expected violations for field count mismatch")
    assert(
      violations.get.exists(_.contains("Field count mismatch")),
      clue(violations.map(_.mkString(", ")))
    )
  }

  test("onlyTypes detects nested struct field count mismatch") {
    val violations = conforms.onlyTypes(
      BQSchema.of(
        BQField(
          "outer",
          BQField.Type.STRUCT,
          BQField.Mode.NULLABLE,
          subFields = List(
            BQField("inner1", BQField.Type.STRING, BQField.Mode.NULLABLE),
            BQField("inner2", BQField.Type.INT64, BQField.Mode.NULLABLE)
          )
        )
      ),
      BQSchema.of(
        BQField(
          "outer",
          BQField.Type.STRUCT,
          BQField.Mode.NULLABLE,
          subFields = List(
            BQField("inner1", BQField.Type.STRING, BQField.Mode.NULLABLE)
          )
        )
      )
    )
    assert(violations.isDefined, "Expected violations for nested field count mismatch")
    assert(
      violations.get.exists(_.contains("Field count mismatch at outer")),
      clue(violations.map(_.mkString(", ")))
    )
  }

}
