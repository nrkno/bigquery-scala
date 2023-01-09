package no.nrk.bigquery

import no.nrk.bigquery.testing.BQSmokeTest
import no.nrk.bigquery.implicits._

class BQReadTest extends BQSmokeTest {
  case class Nested(a: String, b: Long)
  object Nested {
    implicit val bqRead: BQRead[Nested] = BQRead.derived
  }
  case class TestCase(num: String, nesteds: List[Nested])
  object TestCase {
    implicit val bqRead: BQRead[TestCase] = BQRead.derived
  }

  bqCheckTest("nested structure") {
    BQQuery[TestCase](
      bqsql"""SELECT * FROM UNNEST(ARRAY<STRUCT<num STRING, nesteds ARRAY<STRUCT<a STRING, b INT64>>>>[("a", [("b", 10), ("c", 11)])])"""
    )
  }
}
