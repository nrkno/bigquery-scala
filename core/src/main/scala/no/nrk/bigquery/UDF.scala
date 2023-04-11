package no.nrk.bigquery

import no.nrk.bigquery.implicits._

case class UDF(
    name: Ident,
    params: Seq[UDF.Param],
    body: UDF.Body,
    returnType: Option[BQType]
) {
  lazy val definition: BQSqlFrag = {
    val returning = returnType match {
      case Some(returnType) => bqfr" RETURNS $returnType"
      case None => BQSqlFrag.Empty
    }
    bqfr"CREATE TEMP FUNCTION $name${params.map(_.definition).mkFragment("(", ", ", ")")}$returning${body.languageFragment} AS ${body.bodyFragment};"
  }

  def apply(args: BQSqlFrag.Magnet*): BQSqlFrag.Call =
    BQSqlFrag.Call(this, args.map(_.frag))
}

object UDF {
  def apply(
      name: Ident,
      params: Seq[UDF.Param],
      body: BQSqlFrag,
      returnType: Option[BQType]
  ): UDF =
    UDF(name, params, UDF.Body.Sql(body), returnType)

  case class Param(name: Ident, maybeType: Option[BQType]) {
    def definition: BQSqlFrag =
      maybeType match {
        case Some(tpe) => bqfr"$name $tpe"
        case None => bqfr"$name ANY TYPE"
      }
  }
  object Param {
    def apply(name: String, tpe: BQType): Param =
      Param(Ident(name), Some(tpe))
    def untyped(name: String): Param =
      Param(Ident(name), None)
  }

  sealed trait Body {
    def bodyFragment: BQSqlFrag
    def languageFragment: BQSqlFrag
  }
  object Body {
    case class Sql(body: BQSqlFrag) extends Body {
      require(body.asString.startsWith("(") && body.asString.endsWith(")"))
      val languageFragment: BQSqlFrag = BQSqlFrag("")
      override def bodyFragment: BQSqlFrag = bqfr"($body)"

    }
    case class Js(javascriptSnippet: String, gsLibraryPath: Option[String]) extends Body {
      val languageFragment: BQSqlFrag = BQSqlFrag(" LANGUAGE js")
      override def bodyFragment: BQSqlFrag = {
        val jsBody =
          bqfr"""|'''
                 |${BQSqlFrag(javascriptSnippet)}
                 |'''""".stripMargin

        gsLibraryPath
          .map(BQSqlFrag.apply)
          .map(path => bqfr"""OPTIONS ( library="gs://$path" )""") match {
          case None => jsBody
          case Some(libraryOption) =>
            bqfr"""|$jsBody
                   |$libraryOption""".stripMargin
        }
      }
    }
  }

}
