package no.nrk.bigquery

import munit.FunSuite
import no.nrk.bigquery.implicits.BQShowInterpolator
import no.nrk.bigquery.syntax._

class UDFTest extends FunSuite {

  test("render temporary SQL UDF") {
    assertEquals(
      UDF(
        ident"foo",
        Seq(UDF.Param("n", BQType.FLOAT64)),
        UDF.Body.Sql(bqfr"""(n + 1)"""),
        Some(BQType.FLOAT64)
      ).definition.asString,
      """CREATE TEMP FUNCTION foo(n FLOAT64) RETURNS FLOAT64 AS ((n + 1));"""
    )
  }

  test("render temporary javascript UDF") {
    assertEquals(
      UDF(
        ident"foo",
        Seq(UDF.Param("n", BQType.FLOAT64)),
        UDF.Body.Js("return n + 1", None),
        Some(BQType.FLOAT64)
      ).definition.asString,
      """|CREATE TEMP FUNCTION foo(n FLOAT64) RETURNS FLOAT64 LANGUAGE js AS '''
         |return n + 1
         |''';""".stripMargin
    )
  }

  test("render temporary javascript UDF with library path") {
    assertEquals(
      UDF(
        ident"foo",
        Seq(UDF.Param("n", BQType.FLOAT64)),
        UDF.Body.Js("return n + 1", Some("bucket/foo.js")),
        Some(BQType.FLOAT64)
      ).definition.asString,
      """|CREATE TEMP FUNCTION foo(n FLOAT64) RETURNS FLOAT64 LANGUAGE js AS '''
         |return n + 1
         |'''
         |OPTIONS ( library="gs://bucket/foo.js" );""".stripMargin
    )
  }

}
