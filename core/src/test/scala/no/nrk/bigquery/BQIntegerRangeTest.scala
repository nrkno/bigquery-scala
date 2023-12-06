/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import munit.FunSuite

class BQIntegerRangeTest extends FunSuite {
  test("calculate windows interval 1") {
    val TestRange = BQIntegerRange(start = 1, end = 100, interval = 1)
    val (start, end) = TestRange.calculatePartition(5)
    assert(start == 5)
    assert(end == 5)
  }

  test("calculate windows interval 10") {
    val TestRange = BQIntegerRange(start = 1, end = 100, interval = 10)
    val (start, end) = TestRange.calculatePartition(5)
    assert(start == 1)
    assert(end == 10)
  }

  test("outside of range should give full range") {
    val TestRange = BQIntegerRange(start = 1, end = 10, interval = 2)
    val (start, end) = TestRange.calculatePartition(11)
    assert(start == 1)
    assert(end == 10)
  }

}
