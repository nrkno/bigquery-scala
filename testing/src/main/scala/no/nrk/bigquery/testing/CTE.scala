package no.nrk.bigquery.testing

import no.nrk.bigquery._
import no.nrk.bigquery.implicits._

case class CTE(name: Ident, body: BQSqlFrag) {
  require(body.asString.startsWith("(") && body.asString.endsWith(")"))
  def definition: BQSqlFrag = bqfr"$name as $body"
}
