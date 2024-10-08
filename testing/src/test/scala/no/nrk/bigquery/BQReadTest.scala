/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import no.nrk.bigquery.syntax.*
import no.nrk.bigquery.testing.BQSmokeTest

class BQReadTest extends BQSmokeTest(Http4sTestClient.testClient) {
  case class Nested(a: String, b: Long, c: Int, d: Double)
  object Nested {
    implicit val bqRead: BQRead[Nested] = BQRead.derived
  }
  case class TestCase(num: String, nesteds: List[Nested])
  object TestCase {
    implicit val bqRead: BQRead[TestCase] = BQRead.derived
  }
  bqTypeCheckTest("nested structure") {
    BQQuery[TestCase](
      bqsql"""SELECT * FROM UNNEST(ARRAY<STRUCT<num STRING, nesteds ARRAY<STRUCT<a STRING, b INT64, c INT64, d FLOAT64>>>>[("a", [("b", 10, 11, 12.5), ("c", 13, 14, 15.5)])])"""
    )
  }
}
