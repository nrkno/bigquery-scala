/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import com.google.cloud.bigquery.RangePartitioning

case class BQRange(start: Long, end: Long, interval: Long)
object BQRange {
  def fromRangePartitioning(range: RangePartitioning.Range) =
    BQRange(start = range.getStart, end = range.getEnd, interval = range.getInterval)
}
