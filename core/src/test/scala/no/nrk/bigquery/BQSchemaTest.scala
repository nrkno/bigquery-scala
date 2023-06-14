package no.nrk.bigquery

import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.StandardSQLTypeName

class BQSchemaTest extends munit.FunSuite {

  test("extend") {
    val nameField = BQField.struct("name", Mode.REQUIRED)(BQField("id", StandardSQLTypeName.STRING, Mode.REQUIRED))
    val partitionDate = BQField("partitionDate", StandardSQLTypeName.DATE, Mode.REQUIRED)
    val baseSchema = BQSchema.of(nameField)
    val extended = baseSchema.extend(BQSchema.of(partitionDate))
    val extendedWith = baseSchema.extendWith(partitionDate)
    val expected = BQSchema(List(nameField, partitionDate))

    assertEquals(extended, expected)
    assertEquals(extendedWith, expected)
  }

  test("findRequired") {
    val nameField = BQField.struct("name", Mode.REQUIRED)(BQField("id", StandardSQLTypeName.STRING, Mode.REQUIRED))
    val insertedAt = BQField("insertedAt", StandardSQLTypeName.TIMESTAMP, Mode.NULLABLE)
    val schema = BQSchema.of(nameField, insertedAt)

    assertEquals(schema.requiredFields, List(nameField))
  }

  test("findRequired2") {
    val nameField = BQField.struct("name", Mode.REPEATED)(BQField("id", StandardSQLTypeName.STRING, Mode.REQUIRED))
    val insertedAt = BQField("insertedAt", StandardSQLTypeName.TIMESTAMP, Mode.NULLABLE)
    val schema = BQSchema.of(nameField, insertedAt)

    assertEquals(schema.requiredFields, List(nameField))
  }

  test("recursiveNullable") {
    val nameField = BQField.struct("name", Mode.REPEATED)(BQField("id", StandardSQLTypeName.STRING, Mode.REQUIRED))
    val insertedAt = BQField("insertedAt", StandardSQLTypeName.TIMESTAMP, Mode.NULLABLE)
    val schema = BQSchema.of(nameField, insertedAt)

    val nameFieldExpected =
      BQField.struct("name", Mode.REPEATED)(BQField("id", StandardSQLTypeName.STRING, Mode.NULLABLE))

    assertEquals(schema.recursivelyNullable, BQSchema.of(nameFieldExpected, insertedAt))
  }

  test("filter") {
    val nameField = BQField.struct("name", Mode.REPEATED)(BQField("id", StandardSQLTypeName.STRING, Mode.REQUIRED))
    val insertedAt = BQField("insertedAt", StandardSQLTypeName.TIMESTAMP, Mode.NULLABLE)
    val schema = BQSchema.of(nameField, insertedAt)

    val filtered = schema.filter(_.name == "name")

    assertEquals(filtered, BQSchema.of(nameField))
  }

  test("filterNot") {
    val nameField = BQField.struct("name", Mode.REPEATED)(BQField("id", StandardSQLTypeName.STRING, Mode.REQUIRED))
    val insertedAt = BQField("insertedAt", StandardSQLTypeName.TIMESTAMP, Mode.NULLABLE)
    val schema = BQSchema.of(nameField, insertedAt)

    val filtered = schema.filterNot(_.name == "insertedAt")

    assertEquals(filtered, BQSchema.of(nameField))
  }

}
