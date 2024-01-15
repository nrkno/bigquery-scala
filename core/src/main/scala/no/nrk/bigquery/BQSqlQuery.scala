package no.nrk.bigquery

import no.nrk.bigquery.syntax._

import java.time.LocalDate

sealed trait BQSqlQuery

object BQSqlQuery {
  case class PureQuery(frag: BQSqlFrag) extends BQSqlQuery

  case class SessionQuery[A](frag: BQSqlFrag)(implicit pm: QueryParamExtractor[A]) {
    def usingParams(a: A): SessionQuery[Unit] = {
      val lookup = pm.map(a)
      def run(f: BQSqlFrag): BQSqlFrag = f match {
        case x @ BQSqlFrag.Frag(_) => x
        case x @ BQSqlFrag.Combined(values) => BQSqlFrag.Combined(values.map(run))
        case x @ BQSqlFrag.Call(_, _) => x
        case x @ BQSqlFrag.TableRef(_) => x
        case x @ BQSqlFrag.PartitionRef(_) => x
        case x @ BQSqlFrag.FillRef(_) => x
        case x @ BQSqlFrag.FilledTableRef(_) => x
        case x @ BQSqlFrag.Param(name, tpe) => lookup(QueryParamType(name, tpe))
      }
      SessionQuery[Unit](run(frag))
    }

  }

  def pure(frag: BQSqlFrag): Either[String, PureQuery] = {
    val params = frag.collect { case p: BQSqlFrag.Param => p }
    if (params.isEmpty) Right(PureQuery(frag))
    else Left(s"Query can not include parameters. Found:${params.map(_.name).mkString(",")}")
  }

  def session[A](frag: BQSqlFrag)(implicit pm: QueryParamExtractor[A]): Either[String, SessionQuery[A]] = {
    val params = frag.collect { case p: BQSqlFrag.Param => QueryParamType(p.name, p.tpe) }.toSet
    if (params.subsetOf(pm.keys.toSet)) Right(SessionQuery(frag))
    else Left(s"Missing parameters: $params to be part of ${pm.keys.toSet}")
  }

  case class QueryParamType(name: String, tpe: BQField.Type)

  trait QueryParamTypeReader[A] {
    def queryParamType(name: String): QueryParamType
    def toFrag(a: A): BQSqlFrag
  }

  object QueryParamTypeReader {
    implicit val localDate: QueryParamTypeReader[LocalDate] = new QueryParamTypeReader[LocalDate] {
      override def queryParamType(name: String): QueryParamType = QueryParamType(name, BQField.Type.DATE)
      override def toFrag(a: LocalDate): BQSqlFrag = a.bqShow
    }
  }

  trait QueryParamExtractor[A] {
    def keys: List[QueryParamType]
    def map(a: A): Map[QueryParamType, BQSqlFrag]
  }

  object QueryParamExtractor {
    implicit val unitParamMap: QueryParamExtractor[Unit] = new QueryParamExtractor[Unit] {
      override def keys: List[QueryParamType] = List.empty
      override def map(a: Unit): Map[QueryParamType, BQSqlFrag] = Map.empty
    }
  }
}
