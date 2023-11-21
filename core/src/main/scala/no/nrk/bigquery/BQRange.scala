/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import com.google.cloud.bigquery.RangePartitioning

case class BQRange(start: Long, end: Long, interval: Long) {
  def calculatePartition(i: Long): (Long, Long) =
    (start to end).toList.sliding(interval.toInt, interval.toInt).find(_.contains(i)) match {
      case Some(head +: _ :+ tail) => (head, tail)
      case Some(head :: Nil) => (head, head)
      case None => (start, end)
    }
}
object BQRange {
  def fromRangePartitioning(range: RangePartitioning.Range) =
    BQRange(start = range.getStart, end = range.getEnd, interval = range.getInterval)
}
