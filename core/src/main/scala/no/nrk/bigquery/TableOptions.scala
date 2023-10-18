/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import scala.concurrent.duration.FiniteDuration

case class TableOptions(
    partitionFilterRequired: Boolean,
    partitionExpiration: Option[FiniteDuration]
)

object TableOptions {
  val Empty: TableOptions = TableOptions(
    partitionFilterRequired = false,
    partitionExpiration = None
  )
}
