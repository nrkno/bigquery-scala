/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.testing

import no.nrk.bigquery.*
import no.nrk.bigquery.syntax.*

case class CTE(name: Ident, body: BQSqlFrag) {
  require(body.asString.startsWith("(") && body.asString.endsWith(")"))
  def definition: BQSqlFrag = bqfr"$name as $body"
}

case class CTEList(value: List[CTE], recursive: Boolean) {
  def definition: Option[BQSqlFrag] =
    if (value.isEmpty) {
      None
    } else
      Some(bqsql"with ${if (recursive) bqsql"recursive " else bqsql""}" ++ value.map(_.definition).mkFragment(",\n"))
}
