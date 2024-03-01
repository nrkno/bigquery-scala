/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import io.circe.Json
import no.nrk.bigquery.BQRoutine.{Param, Params}
import no.nrk.bigquery.syntax.*
import no.nrk.bigquery.testing.BQUdfSmokeTest

class LiveBqUdfSmokeTest extends BQUdfSmokeTest(Http4sTestClient.testClient) {

  private val doubleUdf = UDF.temporary(
    Ident("xxdouble_TMP"),
    Params(Param("input", BQType.INT64)),
    UDF.Body.Sql(bqsql"(input + input)"),
    Some(BQType.INT64))

  bqCheckCall("test_calling_udf", doubleUdf(bqfr"10"), Json.fromDoubleOrNull(20))
}
