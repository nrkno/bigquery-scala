/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

class BQSchemaTest extends munit.FunSuite {

  test("extend") {
    val nameField =
      BQField.struct("name", BQField.Mode.REQUIRED)(BQField("id", BQField.Type.STRING, BQField.Mode.REQUIRED))
    val partitionDate = BQField("partitionDate", BQField.Type.DATE, BQField.Mode.REQUIRED)
    val baseSchema = BQSchema.of(nameField)
    val extended = baseSchema.extend(BQSchema.of(partitionDate))
    val extendedWith = baseSchema.extendWith(partitionDate)
    val expected = BQSchema(List(nameField, partitionDate))

    assertEquals(extended, expected)
    assertEquals(extendedWith, expected)
  }

  test("findRequired") {
    val nameField =
      BQField.struct("name", BQField.Mode.REQUIRED)(BQField("id", BQField.Type.STRING, BQField.Mode.REQUIRED))
    val insertedAt = BQField("insertedAt", BQField.Type.TIMESTAMP, BQField.Mode.NULLABLE)
    val schema = BQSchema.of(nameField, insertedAt)

    assertEquals(schema.requiredFields, List(nameField))
  }

  test("findRequired2") {
    val nameField =
      BQField.struct("name", BQField.Mode.REPEATED)(BQField("id", BQField.Type.STRING, BQField.Mode.REQUIRED))
    val insertedAt = BQField("insertedAt", BQField.Type.TIMESTAMP, BQField.Mode.NULLABLE)
    val schema = BQSchema.of(nameField, insertedAt)

    assertEquals(schema.requiredFields, List(nameField))
  }

  test("recursiveNullable") {
    val nameField =
      BQField.struct("name", BQField.Mode.REPEATED)(BQField("id", BQField.Type.STRING, BQField.Mode.REQUIRED))
    val insertedAt = BQField("insertedAt", BQField.Type.TIMESTAMP, BQField.Mode.NULLABLE)
    val schema = BQSchema.of(nameField, insertedAt)

    val nameFieldExpected =
      BQField.struct("name", BQField.Mode.REPEATED)(BQField("id", BQField.Type.STRING, BQField.Mode.NULLABLE))

    assertEquals(schema.recursivelyNullable, BQSchema.of(nameFieldExpected, insertedAt))
  }

  test("filter") {
    val nameField =
      BQField.struct("name", BQField.Mode.REPEATED)(BQField("id", BQField.Type.STRING, BQField.Mode.REQUIRED))
    val insertedAt = BQField("insertedAt", BQField.Type.TIMESTAMP, BQField.Mode.NULLABLE)
    val schema = BQSchema.of(nameField, insertedAt)

    val filtered = schema.filter(_.name == "name")

    assertEquals(filtered, BQSchema.of(nameField))
  }

  test("filterNot") {
    val nameField =
      BQField.struct("name", BQField.Mode.REPEATED)(BQField("id", BQField.Type.STRING, BQField.Mode.REQUIRED))
    val insertedAt = BQField("insertedAt", BQField.Type.TIMESTAMP, BQField.Mode.NULLABLE)
    val schema = BQSchema.of(nameField, insertedAt)

    val filtered = schema.filterNot(_.name == "insertedAt")

    assertEquals(filtered, BQSchema.of(nameField))
  }

}
