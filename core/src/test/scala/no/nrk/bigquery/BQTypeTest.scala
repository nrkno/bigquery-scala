/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import munit.FunSuite
import no.nrk.bigquery.syntax.*

class BQTypeTest extends FunSuite {

  test("format array") {
    assertEquals(
      BQType(BQField.Mode.REQUIRED, BQField.Type.ARRAY, List("" -> BQType.STRING)).bqShow,
      bqfr"ARRAY<STRING>")
    assertEquals(BQType.STRING.repeated.bqShow, bqfr"ARRAY<STRING>")
    assertEquals(BQType.struct("foo" -> BQType.STRING).repeated.bqShow, bqfr"ARRAY<STRUCT<foo STRING>>")
  }

  test("format raw types") {
    assertEquals(BQType.BOOL.nullable.bqShow, bqfr"BOOL")
    assertEquals(BQType.INT64.nullable.bqShow, bqfr"INT64")
    assertEquals(BQType.STRING.nullable.bqShow, bqfr"STRING")
  }

  test("format structs") {
    assertEquals(
      BQType.struct("s" -> BQType.STRING, "b" -> BQType.BOOL).bqShow,
      bqfr"STRUCT<s STRING, b BOOL>"
    )
    assertEquals(
      BQType.struct("s" -> BQType.STRING, "b" -> BQType.BOOL).repeated.nullable.bqShow,
      bqfr"STRUCT<s STRING, b BOOL>")
  }

  test("format dictionaries") {
    assertEquals(BQType.StringDictionary.bqShow, bqfr"ARRAY<STRUCT<index INT64, value STRING>>")
    assertEquals(BQType.NumberDictionary.bqShow, bqfr"ARRAY<STRUCT<index INT64, value INT64>>")
  }

}
