/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.data.NonEmptyChain
import munit.FunSuite

import scala.collection.immutable.SortedMap

class JobLabelsTest extends FunSuite {
  test("create label successfully") {
    JobLabels.unsafeFrom("nrkjob" -> "foo")
  }

  test("fail validation") {
    val either = JobLabels.validated(SortedMap("nrkJob" -> "foo"))
    assertEquals(
      either,
      Left(
        NonEmptyChain.of(
          "label key nrkJob can contain only lowercase letters, numeric characters, underscores, and dashes")))
  }
}
