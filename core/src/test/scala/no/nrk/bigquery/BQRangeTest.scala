package no.nrk.bigquery

import munit.FunSuite

class BQRangeTest extends FunSuite {
  test("calculate windows interval 1") {
    val TestRange = BQRange(start = 1, end = 100, interval = 1)
    val (start, end) = TestRange.calculatePartition(5)
    assert(start == 5)
    assert(end == 5)
  }

  test("calculate windows interval 10") {
    val TestRange = BQRange(start = 1, end = 100, interval = 10)
    val (start, end) = TestRange.calculatePartition(5)
    assert(start == 1)
    assert(end == 10)
  }

  test("outside of range should give full range") {
    val TestRange = BQRange(start = 1, end = 10, interval = 2)
    val (start, end) = TestRange.calculatePartition(11)
    assert(start == 1)
    assert(end == 10)
  }


}