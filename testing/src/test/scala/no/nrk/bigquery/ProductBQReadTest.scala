/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import no.nrk.bigquery.syntax.bqShowInterpolator
import no.nrk.bigquery.testing.BQSmokeTest

class ProductBQReadTest extends BQSmokeTest(Http4sTestClient.testClient) {
  case class SubStructure(a: String, b: Long)

  object SubStructure {
    implicit val bqRead: BQRead[SubStructure] = BQRead.derived
  }

  case class ComplexType(num: String, subs: List[SubStructure])

  object ComplexType {
    implicit val bqRead: BQRead[ComplexType] =
      BQRead.forProduct2("numberOfTests", "tests")((n, tests) => ComplexType(n, tests))
  }

  bqTypeCheckTest("nested structure") {
    BQQuery[ComplexType](
      bqsql"""SELECT * FROM UNNEST(ARRAY<STRUCT<numberOfTests STRING, tests ARRAY<STRUCT<a STRING, b INT64>>>>[("a", [("b", 10), ("c", 11)])])"""
    )
  }
}
