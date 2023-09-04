/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package no.nrk.bigquery

import cats.effect.Sync
import com.google.cloud.bigquery.JobId

import java.util.UUID

/** For getting an overview we tag all bigquery jobs (including queries) with a name so we can track the price and
  * duration of individual queries without manual inspection
  */
case class BQJobName private (value: String) extends AnyVal {
  def freshJobId[F[_]](
      locationId: Option[LocationId]
  )(implicit F: Sync[F]): F[JobId] =
    F.delay(
      JobId
        .newBuilder()
        .setJob(s"$value-${UUID.randomUUID}")
        .setLocation(locationId.map(_.value).orNull)
        .build()
    )
  def +(str: String): BQJobName = BQJobName(value + str)
}

object BQJobName {

  /** use a macro to automatically name a job based on the name of the context in which `auto` is called. A typical name
    * is `no_nrk_recommendations_datahub_ecommerce_ECommerceETL_bqFetchRowsForDate`
    */
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
