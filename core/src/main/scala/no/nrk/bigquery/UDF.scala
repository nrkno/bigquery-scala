package no.nrk.bigquery

case class UDF(
    name: Ident,
    params: Seq[UDF.Param],
    body: BQSqlFrag,
    returnType: Option[BQType]
) {
  require(body.asString.startsWith("(") && body.asString.endsWith(")"))

  lazy val definition: BQSqlFrag = {
    val returning = returnType match {
      case Some(returnType) => bqfr" RETURNS $returnType"
      case None             => BQSqlFrag.Empty
    }
    bqfr"CREATE TEMP FUNCTION $name${params.map(_.definition).mkFragment("(", ", ", ")")}$returning AS ($body);"
  }

  def apply(args: BQSqlFrag.Magnet*): BQSqlFrag.Call =
    BQSqlFrag.Call(this, args.map(_.frag))
}

object UDF {
  case class Param(name: Ident, maybeType: Option[BQType]) {
    def definition: BQSqlFrag =
      maybeType match {
        case Some(tpe) => bqfr"$name $tpe"
        case None      => bqfr"$name ANY TYPE"
      }
  }
  object Param {
    def apply(name: String, tpe: BQType): Param =
      Param(Ident(name), Some(tpe))
    def untyped(name: String): Param =
      Param(Ident(name), None)
  }
}
