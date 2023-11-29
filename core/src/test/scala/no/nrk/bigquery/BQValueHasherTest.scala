/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import munit.FunSuite

import java.util.UUID

class BQValueHasherTest extends FunSuite {
  val TestRange = BQRange(start = 0, end = 20, interval = 2)

  test("That the hash is inside our range") {
    assert((1 to 100).forall { _ =>
      val hash = BQValueHasher.stringHasher.hashValue(UUID.randomUUID().toString, TestRange)
      TestRange.start <= hash && hash <= TestRange.end
    })
  }
}
