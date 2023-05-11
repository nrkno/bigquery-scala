package no.nrk.bigquery

import cats.syntax.all._
import munit.FunSuite
import no.nrk.bigquery.BQPartitionType.DatePartitioned
import no.nrk.bigquery.syntax._

import java.time.LocalDate

class BQSqlFragTest extends FunSuite {
  private val udfToString =
    UDF.temporary(
      ident"udf_toString",
      UDF.Param("i", BQType.INT64) :: Nil,
      UDF.Body.Sql(bqfr"(string(i))"),
      Some(BQType.STRING))
  private val udfAddOne =
    UDF.temporary(ident"udf_add1", UDF.Param("i", BQType.INT64) :: Nil, UDF.Body.Sql(bqfr"(i + 1)"), Some(BQType.INT64))

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
      List(UDF.Param("input", BQType.INT64)),
      UDF.Body.Sql(bqsql"(input + input)"),
      Some(BQType.INT64))
    val innerUdf2 = UDF.temporary(
      Ident("cc"),
      List(UDF.Param("input", BQType.INT64)),
      UDF.Body.Sql(bqsql"(input + input)"),
      Some(BQType.INT64))
    val outerUdf = UDF.temporary(
      Ident("aa"),
      List(UDF.Param("input", BQType.INT64)),
      UDF.Body.Sql(bqsql"(${innerUdf1(ident"input")} / ${innerUdf2(bqfr"2")})"),
      Some(BQType.FLOAT64))

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

}
