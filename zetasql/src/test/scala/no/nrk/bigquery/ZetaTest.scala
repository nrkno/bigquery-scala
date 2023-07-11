package no.nrk.bigquery

import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.StandardSQLTypeName
import no.nrk.bigquery.syntax.bqShowInterpolator

import java.time.LocalDate

class ZetaTest extends munit.FunSuite {
  private val table = BQTableDef.Table(
    BQTableId(BQDataset.of(ProjectId("com.example"), "example"), "test"),
    BQSchema.of(
      BQField("partitionDate", StandardSQLTypeName.DATE, Mode.REQUIRED),
      BQField("a", StandardSQLTypeName.STRING, Mode.REQUIRED),
      BQField("b", StandardSQLTypeName.INT64, Mode.REQUIRED),
      BQField("c", StandardSQLTypeName.INT64, Mode.REQUIRED),
      BQField("d", StandardSQLTypeName.INT64, Mode.REQUIRED)
    ),
    BQPartitionType.DatePartitioned(Ident("partitionDate"))
  )

  test("subset select from example") {
    val date = LocalDate.of(2023, 1, 1)

    val query = bqsql"select partitionDate, a, b, c from ${table.assertPartition(date)}"

    val result = ZetaSql.queryFields(query)
    val expected = table.schema.fields.dropRight(1).map(_.recursivelyNullable.withoutDescription)
    assertEquals(result, expected)
  }

  test("all fields should be selected from example") {
    val date = LocalDate.of(2023, 1, 1)

    val query = bqsql"select partitionDate, a, b, c, d from ${table.assertPartition(date)}"

    val result = ZetaSql.queryFields(query)
    val expected = table.schema.fields.map(_.recursivelyNullable.withoutDescription)
    assertEquals(result, expected)
  }
}
