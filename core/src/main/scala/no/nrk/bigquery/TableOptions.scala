/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

case class TableOptions(
    partitionFilterRequired: Boolean
)

object TableOptions {
  val Empty: TableOptions = TableOptions(
    partitionFilterRequired = false
  )
}
