/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import com.google.cloud.bigquery.RangePartitioning

case class BQRange(start: Long, end: Long, interval: Long) {
  def calculatePartition(i: Long): (Long, Long) =
    (start to end).toList
      .sliding(interval.toInt, interval.toInt)
      .find(_.contains(i))
      .collect { case w if w.nonEmpty => (w.head, w.last) }
      .getOrElse((start, end))

}
object BQRange {
  def fromRangePartitioning(range: RangePartitioning.Range) =
    BQRange(start = range.getStart, end = range.getEnd, interval = range.getInterval)

  val default: BQRange = BQRange(start = 0, end = 4000, interval = 1)
}
