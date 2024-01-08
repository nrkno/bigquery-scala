/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

/** For getting an overview we tag all bigquery jobs (including queries) with a name so we can track the price and
  * duration of individual queries without manual inspection
  */
final case class BQJobId(
    projectId: Option[ProjectId],
    locationId: Option[LocationId],
    name: String,
    labels: JobLabels
) {

  def withProjectId(projectId: ProjectId): BQJobId = copy(projectId = Some(projectId))
  def withLocationId(locationId: LocationId): BQJobId = copy(locationId = Some(locationId))
  def withLabels(labels: JobLabels): BQJobId = copy(labels = labels)

  private[bigquery] def withDefaults(defaults: Option[BQClientDefaults]) =
    if (projectId.isEmpty && locationId.isEmpty) {
      copy(projectId = defaults.map(_.projectId), locationId = defaults.map(_.locationId))
    } else this

  def +(str: String): BQJobId = copy(name = name + str)
  def append(str: String): BQJobId = copy(name = name + str)
}

object BQJobId {

  /** use a macro to automatically name a job based on the name of the context in which `auto` is called. A typical name
    * is `no_nrk_bigquery_example_ecommerce_ECommerceETL_bqFetchRowsForDate`
    */
  def auto(implicit enclosing: sourcecode.Enclosing): BQJobId =
    apply(enclosing.value)

  def apply(name: String): BQJobId = apply(name, JobLabels.empty)
  def apply(name: String, labels: JobLabels): BQJobId = BQJobId(None, None, mkName(name), labels)

  private def mkName(str: String): String =
    str
      .replace("anonfun", "") // generated from `sourcecode.Enclosing`
      .replace('.', '_')
      .filter(c => c.isLetterOrDigit || c == '_')
}
