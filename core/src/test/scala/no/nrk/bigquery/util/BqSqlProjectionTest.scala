package no.nrk.bigquery.util

import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.StandardSQLTypeName
import munit.FunSuite
import no.nrk.bigquery._
import no.nrk.bigquery.syntax._

class BqSqlProjectionTest extends FunSuite {

  test("project a struct".ignore) {
    val structField = BQField.struct("foo", Mode.NULLABLE)(
      BQField("keep_me_1", StandardSQLTypeName.STRING, Mode.NULLABLE),
      BQField("keep_me_2", StandardSQLTypeName.STRING, Mode.NULLABLE),
      BQField("drop_me", StandardSQLTypeName.STRING, Mode.NULLABLE),
      BQField("rename_me", StandardSQLTypeName.STRING, Mode.NULLABLE),
      BQField.struct("keep_struct", Mode.NULLABLE)(
        BQField("one", StandardSQLTypeName.STRING, Mode.NULLABLE),
        BQField("two", StandardSQLTypeName.STRING, Mode.NULLABLE)
      ),
      BQField.struct("flatten_struct", Mode.NULLABLE)(
        BQField("one", StandardSQLTypeName.STRING, Mode.NULLABLE),
        BQField("two", StandardSQLTypeName.STRING, Mode.NULLABLE)
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
