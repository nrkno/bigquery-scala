/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import com.google.cloud.bigquery.JobInfo.WriteDisposition as GoogleWriteDisposition
import no.nrk.bigquery.client.google.internal.GoogleTypeHelper

class WriteDispositionTest extends munit.FunSuite {
  test("every WriteDisposition maps to the Google enum of the same name") {
    WriteDisposition.values.foreach(x => assertEquals(GoogleTypeHelper.toGoogleDisposition(x).name(), x.name))
  }

  test("we cover every Google WriteDisposition") {
    val allGoogleValues = GoogleWriteDisposition.values().map(_.name()).toSet
    val allOurValues = WriteDisposition.values.map(_.name).toSet

    assertEquals(allGoogleValues.diff(allOurValues), Set.empty[String])
  }

  test("fromString round-trips") {
    WriteDisposition.values.foreach(x => assertEquals(WriteDisposition.fromString(x.name), x))
  }
}
