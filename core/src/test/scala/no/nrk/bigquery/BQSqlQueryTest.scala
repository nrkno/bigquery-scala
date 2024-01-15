package no.nrk.bigquery

import no.nrk.bigquery.syntax._
import munit.FunSuite
import no.nrk.bigquery.BQSqlQuery._

import java.time.LocalDate

class BQSqlQueryTest extends FunSuite {
  private val executionDateParam = BQSqlFrag.Param("executionDate", BQField.Type.DATE)

  test("session query with unit params") {
    val basicSql = bqfr"select 1"
    val res = BQSqlQuery.session[Unit](basicSql)
    assertEquals(
      res.map(_.usingParams(()).frag),
      Right(basicSql)
    )
  }

  test("session query with case class params") {
    val t = BQTableRef(BQTableId.unsafeFromString("mypoject.mydataset.mytable"), BQPartitionType.NotPartitioned)
    val basicSql = bqfr"select a from $t where pd = $executionDateParam"

    case class MyParams(
        executionDate: LocalDate
    )

    // todo implicit val unitParamMap: QueryParamExtractor[MyParams] = QueryParamExtractor.derive
    implicit val unitParamMap: QueryParamExtractor[MyParams] = new QueryParamExtractor[MyParams] {
      override def keys: List[QueryParamType] = List(executionDateParam).map(p => QueryParamType(p.name, p.tpe))
      override def map(a: MyParams): Map[QueryParamType, BQSqlFrag] =
        Map(QueryParamType(executionDateParam.name, executionDateParam.tpe) -> bqfr"${a.executionDate}")
    }

    val res = BQSqlQuery.session[MyParams](basicSql)

    assertEquals(
      res.map(_.usingParams(MyParams(LocalDate.now())).frag),
      Right(bqfr"select a from $t where pd = ${LocalDate.now()}")
    )
  }

}
