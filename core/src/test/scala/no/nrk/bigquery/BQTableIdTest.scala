package no.nrk.bigquery

import org.scalacheck.Prop

class BQTableIdTest extends munit.ScalaCheckSuite {

  val dataset = BQDataset.unsafeOf(ProjectId.unsafeFromString("com-example"), "test")

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
        assertEquals(obtained, Right(BQTableId(BQDataset(ProjectId(project), dataset, None), table)))
    }
  }
}
