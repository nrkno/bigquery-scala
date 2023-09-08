/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package no.nrk.bigquery

import com.google.cloud.bigquery.TableInfo

case class TableOptions(
    partitionFilterRequired: Boolean
)

object TableOptions {
  val Empty: TableOptions = TableOptions(
    partitionFilterRequired = false
  )

  def fromTableInfo(tableInfo: TableInfo): TableOptions = TableOptions(
    partitionFilterRequired = Option(tableInfo.getRequirePartitionFilter).exists(_.booleanValue())
  )
}
