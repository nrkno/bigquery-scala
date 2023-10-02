/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.effect.IO
import no.nrk.bigquery.syntax._
import com.google.zetasql.toolkit.AnalysisException

import java.time.LocalDate

class ZetaTest extends munit.CatsEffectSuite {
  private lazy val zetaSql = new ZetaSql[IO]

  private val table = BQTableDef.Table(
    BQTableId.unsafeOf(BQDataset.unsafeOf(ProjectId("com-example"), "example"), "test"),
    BQSchema.of(
      BQField("partitionDate", BQField.Type.DATE, BQField.Mode.REQUIRED),
      BQField("a", BQField.Type.STRING, BQField.Mode.REQUIRED),
      BQField("b", BQField.Type.INT64, BQField.Mode.REQUIRED),
      BQField("c", BQField.Type.INT64, BQField.Mode.REQUIRED),
      BQField("d", BQField.Type.INT64, BQField.Mode.REQUIRED)
    ),
    BQPartitionType.DatePartitioned(Ident("partitionDate"))
  )

  test("parses select 1") {
    zetaSql.analyzeFirst(bqsql"select 1").map(_.isRight).assertEquals(true)
  }

  test("fails to parse select from foo") {
    zetaSql.analyzeFirst(bqsql"select from foo").flatMap(IO.fromEither).intercept[AnalysisException]
  }

  test("subset select from example") {
    val date = LocalDate.of(2023, 1, 1)

    val query = bqsql"select partitionDate, a, b, c from ${table.assertPartition(date)}"

    val expected = table.schema.fields.dropRight(1).map(_.recursivelyNullable.withoutDescription)
    zetaSql.queryFields(query).assertEquals(expected)
  }

  test("all fields should be selected from example") {
    val date = LocalDate.of(2023, 1, 1)

    val query = bqsql"select partitionDate, a, b, c, d from ${table.assertPartition(date)}"

    val expected = table.schema.fields.map(_.recursivelyNullable.withoutDescription)
    zetaSql.queryFields(query).assertEquals(expected)
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
      (table.schema.fields.dropRight(2) ++ List(BQField("nullableCs", BQField.Type.INT64, BQField.Mode.NULLABLE)))
        .map(_.recursivelyNullable.withoutDescription)

    zetaSql.queryFields(query).assertEquals(expected)
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
      (table.schema.fields.dropRight(2) ++ List(BQField("nullableCs", BQField.Type.INT64, BQField.Mode.NULLABLE)))
        .map(_.recursivelyNullable.withoutDescription)

    zetaSql
      .parseAndBuildAnalysableFragment(query, List(table))
      .flatMap(zetaSql.queryFields)
      .assertEquals(expected)
  }

  override def munitTestTransforms: List[TestTransform] =
    super.munitTestTransforms ++ List(
      new TestTransform(
        "disabled-for-aarch64",
        test =>
          if ("aarch64" == System.getProperty("os.arch").toLowerCase)
            test
              .withBody[Boolean] { () =>
                println("Test is Disabled for \"aarch64\"")
                true
              }
              .asInstanceOf[Test]
          else test
      ))
}
