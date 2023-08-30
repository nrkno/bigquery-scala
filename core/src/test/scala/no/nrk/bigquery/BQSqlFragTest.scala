/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.syntax.all._
import munit.FunSuite
import no.nrk.bigquery.BQPartitionType.DatePartitioned
import no.nrk.bigquery.syntax._
import no.nrk.bigquery.Routines.{Param, Params}

import java.time.LocalDate

class BQSqlFragTest extends FunSuite {
  private val udfToString =
    UDF.temporary(
      ident"udf_toString",
      Params(Param("i", BQType.INT64)),
      UDF.Body.Sql(bqfr"(string(i))"),
      Some(BQType.STRING))

  private val udfAddOne =
    UDF.temporary(
      ident"udf_add1",
      Params(Param("i", BQType.INT64)),
      UDF.Body.Sql(bqfr"(i + 1)"),
      Some(BQType.INT64)
    )

  test("collect nested UDFs") {
    val udfIdents = bqfr"select ${udfToString(udfAddOne(bqfr"1"))}"
      .collect { case BQSqlFrag.Call(udf, _) => udf }
      .map(_.name)
      .sortBy(_.show)

    assertEquals(udfIdents, udfAddOne.name :: udfToString.name :: Nil)
  }

  test("collect UDF used in body in other UDFs") {
    val innerUdf1 = UDF.temporary(
      Ident("bb"),
      Params(Param("input", BQType.INT64)),
      UDF.Body.Sql(bqsql"(input + input)"),
      Some(BQType.INT64))
    val innerUdf2 = UDF.temporary(
      Ident("cc"),
      Params(Param("input", BQType.INT64)),
      UDF.Body.Sql(bqsql"(input + input)"),
      Some(BQType.INT64))
    val outerUdf = UDF.temporary(
      Ident("aa"),
      Params(Param("input", BQType.INT64)),
      UDF.Body.Sql(bqsql"(${innerUdf1(ident"input")} / ${innerUdf2(bqfr"2")})"),
      Some(BQType.FLOAT64)
    )

    val udfIdents = bqsql"select ${outerUdf(1)}"
      .collect { case BQSqlFrag.Call(udf, _) => udf }
      .map(_.name)

    assertEquals(udfIdents, innerUdf1.name :: innerUdf2.name :: outerUdf.name :: Nil)
  }

  test("collect partitions in order") {
    val date = LocalDate.of(2023, 1, 1)
    def tableId(name: String) = BQTableId(BQDataset(ProjectId("p1"), "d1", None), name)

    val t1 = BQTableRef(tableId("t1"), DatePartitioned(Ident("column1")))
    val t2 = BQTableRef(tableId("t2"), DatePartitioned(Ident("column1")))
    val t3 = BQTableRef(tableId("t3"), DatePartitioned(Ident("column1")))
    val combineFr = bqfr"(select * from ${t3.assertPartition(date)} where column2 = 2)"

    val tableIds =
      bqfr"""|select 1
             |from ${t2.assertPartition(date)} t2
             |join ${t1.assertPartition(date)} t1 on t1.id = t2.id
             |join $combineFr t3 on t3.id = t2.id
             |""".stripMargin
        .collect { case BQSqlFrag.PartitionRef(ref) => ref.wholeTable.tableId }

    assertEquals(tableIds, t2.tableId :: t1.tableId :: t3.tableId :: Nil)
  }

  test("collect UDF used in body in other fragments, but not child BQFill") {
    case class JobKey(value: String) extends JobKeyBQ

    val outerUdf1 = UDF.temporary(
      Ident("outer1"),
      Params(Param("input", BQType.INT64)),
      UDF.Body.Sql(bqsql"(2.0)"),
      Some(BQType.FLOAT64))

    val outerUdf2 = UDF.temporary(
      Ident("outer2"),
      Params(Param("input", BQType.INT64)),
      UDF.Body.Sql(bqsql"(1.0)"),
      Some(BQType.FLOAT64))

    val fill2 =
      BQFill(JobKey("hello2"), mkTable("bree"), bqsql"select ${outerUdf2(1)}", LocalDate.of(2023, 1, 1))

    val fill1 = BQFill(
      JobKey("hello"),
      mkTable("baz"),
      bqsql"select ${outerUdf1(1)} from $fill2",
      LocalDate.of(2023, 1, 1)
    )

    val udfIdents = fill1.query
      .collect { case BQSqlFrag.Call(udf, _) => udf }
      .map(_.name)

    assertEquals(udfIdents, outerUdf1.name :: Nil)
  }

  def mkTable(name: String) = {
    val partitionField = BQField("partitionDate", BQField.Type.DATE, BQField.Mode.REQUIRED)

    BQTableDef.Table(
      BQTableId.unsafeOf(BQDataset.unsafeOf(ProjectId("foo"), "bar"), name),
      BQSchema.of(partitionField, BQField("num", BQField.Type.FLOAT64, BQField.Mode.REQUIRED)),
      BQPartitionType.DatePartitioned(partitionField.ident)
    )
  }
}
