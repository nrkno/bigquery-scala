package no.nrk.bigquery

import munit.FunSuite

import java.util.UUID

class BQValueHasherTest extends FunSuite {
  val TestRange = BQRange(start = 0, end = 20, interval = 2)
  val Hasher = BQValueHasher.ShaHasher(TestRange)

  test("That the hash is inside our range") {
    assert((1 to 100).forall { _ =>
      val hash = Hasher.hashValue(UUID.randomUUID().toString)
      TestRange.start <= hash && hash <= TestRange.end
    })
  }
}
