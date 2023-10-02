/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import com.google.cloud.bigquery.{Field, StandardSQLTypeName}

class BQFieldTypeModeTest extends munit.FunSuite {
  test("convert to StandardSQLTypeName") {
    BQField.Type.values.map { x =>
      try
        StandardSQLTypeName.valueOf(x.name)
      catch {
        case _: Exception => fail(s"$x is not in StandardSQLTypeName")
      }
    }
    val allStandardValues = StandardSQLTypeName.values().map(_.name()).toSet
    val allOurTypes = BQField.Type.values.map(_.name).toSet

    assert(allStandardValues.diff(allOurTypes).isEmpty)
  }
  test("convert to BQField.Type") {
    StandardSQLTypeName.values.map { x =>
      BQField.Type.fromString(x.name) match {
        case Some(value) => assertEquals(value.name, x.name())
        case None => fail(s"$x is not in BQField.Type")
      }
    }
  }

  test("convert to Field.Mode") {
    BQField.Mode.values.map { x =>
      try
        Field.Mode.valueOf(x.name)
      catch {
        case _: Exception => fail(s"$x is not in Field.Mode")
      }
    }
    val allStandardValues = Field.Mode.values().map(_.name()).toSet
    val allOurTypes = BQField.Mode.values.map(_.name).toSet

    assert(allStandardValues.diff(allOurTypes).isEmpty)
  }
  test("convert to BQField.Mode") {
    Field.Mode.values.map { x =>
      BQField.Mode.fromString(x.name) match {
        case Some(value) => assertEquals(value.name, x.name())
        case None => fail(s"$x is not in BQField.Mode")
      }
    }
  }
}
