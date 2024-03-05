/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.effect.IO
import munit.CatsEffectSuite
import no.nrk.bigquery.syntax.*
import no.nrk.bigquery.testing.BigQueryTestClient

import java.time.{Instant, LocalDate, LocalTime}

/** By writing roundtrip test we test instances for both `BQShow` and `BQRead` at the same time.
  *
  * I didn't think too long about adding too many cases, feel free to add more
  */
class RoundtripTest extends CatsEffectSuite {
  def roundtripQuery[P: BQShow: BQRead](ps: List[P]): BQQuery[P] =
    BQQuery(bqfr"select * from unnest([${ps.mkFragment(", ")}])")

  def roundtrip[P: BQShow: BQRead](expectedValues: P*): IO[Unit] =
    BigQueryTestClient
      .cachingClient(BigQueryTestClient.queryCachePath, GoogleTestClient.testClient)
      .use(
        _.synchronousQuery(
          BQJobId.auto,
          roundtripQuery(expectedValues.toList)
        ).compile.toVector
      )
      .map(actualValues => assertEquals(actualValues, expectedValues.toVector))

  test("roundtrip strings") {
    roundtrip(
      StringValue(""),
      StringValue("a"),
      StringValue("ø"),
      StringValue("你好世界"),
      StringValue("😀")
      // todo: implement escaping
      //        "\\",
      //        "\n",
      //        "\\q",
    )
  }

  test("roundtrip instants") {
    roundtrip(
      // note: further precision is rejected by BQ at query-time. java seems to have no problem with it
      Instant.parse("2020-01-02T07:12:34.123456Z"),
      Instant.parse("1969-01-02T07:12:34Z"),
      Instant.parse("2069-01-02T07:12:34Z")
    )
  }

  test("roundtrip longs") {
    roundtrip(Long.MaxValue, 0L, Long.MinValue)
  }

  test("roundtrip times") {
    roundtrip(
      // note: nanosecond precision is not available in BQ. any further precision behind the three nines will be truncated
      LocalTime.of(1, 23, 59, 999000),
      LocalTime.of(0, 1, 22)
    )
  }

  test("roundtrip dates") {
    roundtrip(
      LocalDate.of(2020, 12, 31),
      LocalDate.of(2020, 1, 1)
    )
  }
}
