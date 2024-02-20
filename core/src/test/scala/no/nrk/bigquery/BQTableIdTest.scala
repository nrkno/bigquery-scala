/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import org.scalacheck.Prop

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

class BQTableIdTest extends munit.ScalaCheckSuite {

  val dataset = BQDataset.Ref.unsafeOf(ProjectId.unsafeFromString("com-example"), "test")

  property("valid tableId") {
    Prop.forAll(Generators.validTableIdGen) { (input: String) =>
      val obtained = BQTableId.of(dataset, input)
      assertEquals(obtained, Right(BQTableId(dataset, input)))
    }
  }

  property("fromString") {
    Prop.forAll(Generators.validProjectIdGen, Generators.validDatasetIdGen, Generators.validTableIdGen) {
      (project: String, dataset: String, table: String) =>
        val obtained = BQTableId.fromString(s"${project}.${dataset}.${table}")
        assertEquals(obtained, Right(BQTableId(BQDataset.Ref(ProjectId(project), dataset), table)))
    }
  }

  property("fromStringUrlEncoded") {
    Prop.forAll(Generators.validProjectIdGen, Generators.validDatasetIdGen, Generators.validTableIdGen) {
      (project: String, dataset: String, table: String) =>
        def encode(s: String) = URLEncoder.encode(s, StandardCharsets.UTF_8.name())

        val obtained =
          BQTableId.fromString(s"projects/${encode(project)}/datasets/${encode(dataset)}/tables/${encode(table)}")
        assertEquals(obtained, Right(BQTableId(BQDataset.Ref(ProjectId(project), dataset), table)))
    }
  }

  test("examples must work") {
    val ids = List("cloudaudit_googleapis_com_data_access_*", "service_daily_example_v01$20200801")

    for (id <- ids) {
      val obtained = BQTableId.of(dataset, id)
      assertEquals(obtained, Right(BQTableId(dataset, id)))
    }
  }
}
