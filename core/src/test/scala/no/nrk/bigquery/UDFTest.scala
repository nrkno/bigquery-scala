package no.nrk.bigquery

import munit.FunSuite
import no.nrk.bigquery.syntax._

class UDFTest extends FunSuite {

  test("render temporary SQL UDF") {
    assertEquals(
      UDF
        .temporary(
          ident"foo",
          List(UDF.Param("n", BQType.FLOAT64)),
          UDF.Body.Sql(bqfr"""(n + 1)"""),
          Some(BQType.FLOAT64)
        )
        .definition
        .asString,
      """CREATE TEMP FUNCTION foo(n FLOAT64) RETURNS FLOAT64 AS ((n + 1));"""
    )
  }

  test("render temporary javascript UDF") {
    assertEquals(
      UDF
        .temporary(
          ident"foo",
          List(UDF.Param("n", BQType.FLOAT64)),
          UDF.Body.Js("return n + 1", List.empty),
          Some(BQType.FLOAT64)
        )
        .definition
        .asString,
      """|CREATE TEMP FUNCTION foo(n FLOAT64) RETURNS FLOAT64 LANGUAGE js AS '''
         |return n + 1
         |''';""".stripMargin
    )
  }

  test("render temporary javascript UDF with library path") {
    assertEquals(
      UDF
        .temporary(
          ident"foo",
          List(UDF.Param("n", BQType.FLOAT64)),
          UDF.Body.Js("return n + 1", List("bucket/foo.js")),
          Some(BQType.FLOAT64)
        )
        .definition
        .asString,
      """|CREATE TEMP FUNCTION foo(n FLOAT64) RETURNS FLOAT64 LANGUAGE js AS '''
         |return n + 1
         |'''
         |OPTIONS ( library=["gs://bucket/foo.js"] );""".stripMargin
    )
  }

  test("render temporary udf call") {
    val udf = UDF
      .temporary(
        ident"fnName",
        List(UDF.Param("n", BQType.FLOAT64)),
        UDF.Body.Js("return n + 1", List("bucket/foo.js")),
        Some(BQType.FLOAT64)
      )
    assertEquals(bqfr"${udf(1d)}".asString, "fnName(1.0)")
  }

  test("render persistent call") {
    val udf = UDF
      .persistent(
        ident"fnName",
        BQDataset(ProjectId("p1"), "ds1", None),
        List(UDF.Param("n", BQType.FLOAT64)),
        UDF.Body.Js("return n + 1", List("bucket/foo.js")),
        Some(BQType.FLOAT64)
      )

    assertEquals(bqfr"${udf(1d)}".asString, "`p1.ds1.fnName`(1.0)")
  }

  test("render udf ref call") {
    val udf = UDF.reference(
      ident"fnName",
      BQDataset(ProjectId("p1"), "ds1", None),
      List(UDF.Param("n", BQType.FLOAT64)),
      Some(BQType.FLOAT64)
    )

    assertEquals(bqfr"${udf(1d)}".asString, "`p1.ds1.fnName`(1.0)")
  }

}
