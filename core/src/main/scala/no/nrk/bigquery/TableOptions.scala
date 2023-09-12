/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
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
