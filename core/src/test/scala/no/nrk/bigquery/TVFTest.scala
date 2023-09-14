/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import no.nrk.bigquery._
import no.nrk.bigquery.syntax._
import munit.FunSuite

class TVFTest extends FunSuite {

  test("calling TVF with args") {
    val tvf = TVF(
      TVF.TVFId(BQDataset.unsafeOf(ProjectId.unsafeFromString("my-test-project"), "my_cool_ds"), ident"tada"),
      BQPartitionType.NotPartitioned,
      BQRoutine.Params(BQRoutine.Param(ident"name", Some(BQType.STRING))),
      bqfr"select name",
      BQSchema.of(BQField("name", BQField.Type.STRING, BQField.Mode.NULLABLE))
    )

    val rendered = bqfr"${tvf(ident"bar")}".asString
    assertEquals(rendered, "`my-test-project.my_cool_ds.tada`(bar)")
  }
}
