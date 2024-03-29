/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import no.nrk.bigquery.BQRoutine.{Param, Params}
import no.nrk.bigquery.syntax.*
import no.nrk.bigquery.testing.BQSmokeTest

class LiveTempUdfTest extends BQSmokeTest(Http4sTestClient.testClient) {
  val udf1 = UDF.temporary(
    Ident("xxdouble_TMP"),
    Params(Param("input", BQType.INT64)),
    UDF.Body.Sql(bqsql"(input + input)"),
    Some(BQType.INT64))
  val udf2 = UDF.temporary(
    Ident("half_TMP"),
    Params(Param("input", BQType.INT64)),
    UDF.Body.Sql(bqsql"(${udf1(Ident("input"))} / 2)"),
    Some(BQType.FLOAT64))

  bqTypeCheckTest("query with UDF") {
    BQQuery[Double](bqsql"select ${udf2(1)}")
  }
}
