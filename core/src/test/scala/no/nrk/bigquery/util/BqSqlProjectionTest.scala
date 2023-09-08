/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package no.nrk.bigquery.util

import munit.FunSuite
import no.nrk.bigquery._
import no.nrk.bigquery.syntax._

class BqSqlProjectionTest extends FunSuite {

  test("project a struct".ignore) {
    val structField = BQField.struct("foo", BQField.Mode.NULLABLE)(
      BQField("keep_me_1", BQField.Type.STRING, BQField.Mode.NULLABLE),
      BQField("keep_me_2", BQField.Type.STRING, BQField.Mode.NULLABLE),
      BQField("drop_me", BQField.Type.STRING, BQField.Mode.NULLABLE),
      BQField("rename_me", BQField.Type.STRING, BQField.Mode.NULLABLE),
      BQField.struct("keep_struct", BQField.Mode.NULLABLE)(
        BQField("one", BQField.Type.STRING, BQField.Mode.NULLABLE),
        BQField("two", BQField.Type.STRING, BQField.Mode.NULLABLE)
      ),
      BQField.struct("flatten_struct", BQField.Mode.NULLABLE)(
        BQField("one", BQField.Type.STRING, BQField.Mode.NULLABLE),
        BQField("two", BQField.Type.STRING, BQField.Mode.NULLABLE)
      )
    )
    val projection = BqSqlProjection(structField) {
      case BQField("drop_me", _, _, _, _, _) => BqSqlProjection.Drop
      case BQField("rename_me", _, _, _, _, _) => BqSqlProjection.Rename(ident"renamed")
      case BQField("flatten_struct", _, _, _, _, _) => BqSqlProjection.Flatten(Some(ident"bar"))
      case _ => BqSqlProjection.Keep
    }.get

    assertEquals(
      projection.fragment.asString,
      """|(SELECT AS STRUCT # start struct foo (dropped drop_me)
         |  foo.keep_me_1,
         |  foo.keep_me_2,
         |  foo.rename_me renamed,
         |  foo.keep_struct keep_struct,
         |  foo.flatten_struct.one barOne,
         |  foo.flatten_struct.two barTwo
         |)""".stripMargin
    )
  }

}
