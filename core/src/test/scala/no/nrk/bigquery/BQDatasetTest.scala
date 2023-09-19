/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import org.scalacheck._

class BQDatasetTest extends munit.ScalaCheckSuite {
  val project = ProjectId.unsafeFromString("test-123")

  property("valid dataset") {
    Prop.forAll(Generators.validDatasetIdGen) { (ident: String) =>
      assertEquals(BQDataset.of(project, ident), Right(BQDataset(project, ident, None)))
    }
  }

  property("invalid dataset") {
    val gen = for {
      sep <- Gen.oneOf("", "-", "$", "@", ".")
      alpha <-
        if (sep.nonEmpty) Generators.shorterThanAlphaNum(1024).filterNot(_.isEmpty).map(_ + sep)
        else Gen.stringOfN(1025, Gen.alphaNumChar)
    } yield alpha

    Prop.forAll(gen) { (ident: String) =>
      BQDataset.of(project, ident).isLeft
    }
  }
}
