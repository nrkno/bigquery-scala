/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import no.nrk.bigquery.syntax.bqShowInterpolator
import no.nrk.bigquery.testing.BQSmokeTest

class ProductBQReadTest extends BQSmokeTest(Http4sTestClient.testClient) {
  case class Nested(a: String, b: Long)

  object Nested {
    implicit val bqRead: BQRead[Nested] = BQRead.derived
  }

  case class TestCase(num: String, nested: List[Nested])

  object TestCase {
    implicit val bqRead: BQRead[TestCase] = BQRead.forProduct2("numberOfTests", "tests")((n, tests) => TestCase(n, tests))
  }

  bqTypeCheckTest("nested structure") {
    BQQuery[TestCase](
      bqsql"""SELECT * FROM UNNEST(ARRAY<STRUCT<numberOfTests STRING, tests ARRAY<STRUCT<a STRING, b INT64>>>>[("a", [("b", 10), ("c", 11)])])"""
    )
  }
}