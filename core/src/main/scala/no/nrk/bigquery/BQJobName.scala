/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

/** For getting an overview we tag all bigquery jobs (including queries) with a name so we can track the price and
  * duration of individual queries without manual inspection
  */
//todo: consider merging with BQJobId
case class BQJobName private (value: String) extends AnyVal {
  def +(str: String): BQJobName = BQJobName(value + str)
}

object BQJobName {

  /** use a macro to automatically name a job based on the name of the context in which `auto` is called. A typical name
    * is `no_nrk_recommendations_datahub_ecommerce_ECommerceETL_bqFetchRowsForDate`
    */
  @deprecated(message = "Use BQJobId.auto instead", since = "0.12.0")
  def auto(implicit enclosing: sourcecode.Enclosing): BQJobName =
    apply(enclosing.value)

  def apply(str: String): BQJobName =
    new BQJobName(
      str
        .replace("anonfun", "") // generated from `sourcecode.Enclosing`
        .replace('.', '_')
        .filter(c => c.isLetterOrDigit || c == '_')
    )
}
