/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package no.nrk.bigquery

import munit.FunSuite
import no.nrk.bigquery.syntax._
import shapeless.{Sized, _0}

class UDFTest extends FunSuite {

  test("render temporary SQL UDF") {
    assertEquals(
      UDF
        .temporary(
          ident"foo",
          UDF.Params.of(UDF.Param("n", BQType.FLOAT64)),
          UDF.Body.Sql(bqfr"""(n + 1)"""),
          Some(BQType.FLOAT64)
        )
        .definition
        .asString,
      """CREATE TEMP FUNCTION foo(n FLOAT64) RETURNS FLOAT64 AS ((n + 1));"""
    )
  }

  test("render temporary SQL UDF without params") {
    assertEquals(
      UDF
        .temporary(
          ident"foo",
          UDF.Params.empty,
          UDF.Body.Sql(bqfr"""(n + 1)"""),
          Some(BQType.FLOAT64)
        )
        .definition
        .asString,
      """CREATE TEMP FUNCTION foo() RETURNS FLOAT64 AS ((n + 1));"""
    )
  }

  test("render call with empty parameters") {
    val foo: UDF.Temporary[_0] = UDF
      .temporary(
        ident"foo",
        UDF.Params.empty,
        UDF.Body.Sql(bqfr"""(n + 1)"""),
        Some(BQType.FLOAT64)
      )

    assertEquals(
      foo().asString,
      """foo()"""
    )
  }

  test("render temporary javascript UDF") {
    assertEquals(
      UDF
        .temporary(
          ident"foo",
          UDF.Params.of(UDF.Param("n", BQType.FLOAT64)),
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
          UDF.Params.of(UDF.Param("n", BQType.FLOAT64)),
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
        Sized(UDF.Param("n", BQType.FLOAT64)),
        UDF.Body.Js("return n + 1", List("bucket/foo.js")),
        Some(BQType.FLOAT64)
      )
    assertEquals(bqfr"${udf(Sized(1d))}".asString, "fnName(1.0)")
  }

  test("render persistent call") {
    val udf = UDF
      .persistent(
        ident"fnName",
        BQDataset(ProjectId("p1"), "ds1", None),
        UDF.Params.of(UDF.Param("n", BQType.FLOAT64)),
        UDF.Body.Js("return n + 1", List("bucket/foo.js")),
        Some(BQType.FLOAT64)
      )

    assertEquals(bqfr"${udf(Sized(1d))}".asString, "`p1.ds1.fnName`(1.0)")
  }

  test("render udf ref call") {
    val udf = UDF.reference(
      ident"fnName",
      BQDataset(ProjectId("p1"), "ds1", None),
      UDF.Params.of(UDF.Param("n", BQType.FLOAT64)),
      Some(BQType.FLOAT64)
    )

    assertEquals(bqfr"${udf.apply(Sized(1d))}".asString, "`p1.ds1.fnName`(1.0)")
  }

}
