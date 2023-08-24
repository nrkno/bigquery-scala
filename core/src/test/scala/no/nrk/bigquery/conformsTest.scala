package no.nrk.bigquery

import com.google.cloud.bigquery.{Field, StandardSQLTypeName}
import munit.FunSuite

class conformsTest extends FunSuite {
  test("failing conform by type position") {
    val violations = conforms.onlyTypes(
      BQSchema.of(
        BQField("one", StandardSQLTypeName.STRING, Field.Mode.NULLABLE),
        BQField("two", StandardSQLTypeName.INT64, Field.Mode.NULLABLE)
      ),
      BQSchema.of(
        BQField("one", StandardSQLTypeName.INT64, Field.Mode.NULLABLE),
        BQField("two", StandardSQLTypeName.STRING, Field.Mode.NULLABLE)
      )
    )
    assertEquals(violations.map(_.length), Some(2), clue(violations.map(_.mkString(", "))))
  }

  test("passing conform by type position") {
    val violations = conforms.onlyTypes(
      BQSchema.of(
        BQField("one", StandardSQLTypeName.STRING, Field.Mode.NULLABLE),
        BQField("two", StandardSQLTypeName.INT64, Field.Mode.NULLABLE)
      ),
      BQSchema.of(
        BQField("one", StandardSQLTypeName.STRING, Field.Mode.NULLABLE),
        BQField("two", StandardSQLTypeName.INT64, Field.Mode.NULLABLE)
      )
    )
    assertEquals(violations.map(_.length), None, clue(violations.map(_.mkString(", "))))
  }

  test("passing conform by name and type") {
    val violations = conforms.typesAndName(
      BQSchema.of(
        BQField("one", StandardSQLTypeName.STRING, Field.Mode.NULLABLE),
        BQField("two", StandardSQLTypeName.INT64, Field.Mode.NULLABLE)
      ),
      BQSchema.of(
        BQField("two", StandardSQLTypeName.INT64, Field.Mode.NULLABLE),
        BQField("one", StandardSQLTypeName.STRING, Field.Mode.NULLABLE)
      )
    )
    assertEquals(violations.map(_.length), None, clue(violations.map(_.mkString(", "))))
  }

  test("failing conform by name and type") {
    val violations = conforms.typesAndName(
      BQSchema.of(
        BQField("one", StandardSQLTypeName.STRING, Field.Mode.NULLABLE),
        BQField("two", StandardSQLTypeName.INT64, Field.Mode.NULLABLE)
      ),
      BQSchema.of(
        BQField("two", StandardSQLTypeName.STRING, Field.Mode.NULLABLE),
        BQField("one", StandardSQLTypeName.INT64, Field.Mode.NULLABLE)
      )
    )
    assertEquals(violations.map(_.length), Some(2), clue(violations.map(_.mkString(", "))))
  }

}
