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

}
