/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import org.scalacheck.Prop

class ProjectIdTest extends munit.ScalaCheckSuite {
  test("invalid projectId") {
    assert(ProjectId.fromString("abc-def-").isLeft)
    assert(ProjectId.fromString("1abcdef_").isLeft)
    assert(ProjectId.fromString("1fffffff").isLeft)
    assert(ProjectId.fromString("zxyvx").isLeft)
    assert(ProjectId.fromString("1zxyvxx").isLeft)
    assert(ProjectId.fromString("zxyvxxZ").isLeft)
    assert(ProjectId.fromString("abcdefghijklmnopqrstuvwxyz12345").isLeft)
  }

  property("valid projectId") {
    Prop.forAll(Generators.validProjectIdGen) { (input: String) =>
      assertEquals(ProjectId.fromString(input), Right(ProjectId.apply(input)))
    }
  }
}
