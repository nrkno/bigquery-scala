package no.nrk.bigquery

import cats.effect.IO
import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.StandardSQLTypeName
import no.nrk.bigquery.syntax._
import com.google.zetasql.SqlException

import java.time.LocalDate

class ZetaTest extends munit.CatsEffectSuite {
  private val table = BQTableDef.Table(
    BQTableId(BQDataset.of(ProjectId("com-example"), "example"), "test"),
    BQSchema.of(
      BQField("partitionDate", StandardSQLTypeName.DATE, Mode.REQUIRED),
      BQField("a", StandardSQLTypeName.STRING, Mode.REQUIRED),
      BQField("b", StandardSQLTypeName.INT64, Mode.REQUIRED),
      BQField("c", StandardSQLTypeName.INT64, Mode.REQUIRED),
      BQField("d", StandardSQLTypeName.INT64, Mode.REQUIRED)
    ),
    BQPartitionType.DatePartitioned(Ident("partitionDate"))
  )

  test("parses select 1") {
    ZetaSql.parse(bqsql"select 1").map(_.isRight).assertEquals(true)
  }

  test("fails to parse select from foo") {
    ZetaSql.parse(bqsql"select from foo").flatMap(IO.fromEither).intercept[SqlException]
  }

  test("subset select from example") {
    val date = LocalDate.of(2023, 1, 1)

    val query = bqsql"select partitionDate, a, b, c from ${table.assertPartition(date)}"

    val expected = table.schema.fields.dropRight(1).map(_.recursivelyNullable.withoutDescription)
    ZetaSql.queryFields(query).assertEquals(expected)
  }

  test("all fields should be selected from example") {
    val date = LocalDate.of(2023, 1, 1)

    val query = bqsql"select partitionDate, a, b, c, d from ${table.assertPartition(date)}"

    val expected = table.schema.fields.map(_.recursivelyNullable.withoutDescription)
    ZetaSql.queryFields(query).assertEquals(expected)
  }

  test("CTE selections") {
    val query =
      bqsql"""|with data as (
              | select partitionDate, a, b, c from ${table.unpartitioned}
              |),
              | grouped as (
              |   select partitionDate, a, b, COUNTIF(c is null) as nullableCs from data
              |   group by 1, 2, 3
              | )
              |select * from grouped
              |""".stripMargin

    val expected =
      (table.schema.fields.dropRight(2) ++ List(BQField("nullableCs", StandardSQLTypeName.INT64, Mode.NULLABLE)))
        .map(_.recursivelyNullable.withoutDescription)

    ZetaSql.queryFields(query).assertEquals(expected)
  }

  test("parse then build analysis") {
    val query =
      """|with data as (
         | select partitionDate, a, b, c from `com-example.example.test`
         |),
         | grouped as (
         |   select partitionDate, a, b, COUNTIF(c is null) as nullableCs from data
         |   group by 1, 2, 3
         | )
         |select * from grouped
         |""".stripMargin

    val expected =
      (table.schema.fields.dropRight(2) ++ List(BQField("nullableCs", StandardSQLTypeName.INT64, Mode.NULLABLE)))
        .map(_.recursivelyNullable.withoutDescription)

    ZetaSql
      .parseAndBuildAnalysableFragment(query, List(table))
      .flatMap(ZetaSql.queryFields)
      .assertEquals(expected)
  }
}
