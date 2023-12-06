/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

case class BQIntegerRange(start: Long, end: Long, interval: Long) {
  def calculatePartition(i: Long): (Long, Long) =
    (start to end).toList
      .sliding(interval.toInt, interval.toInt)
      .find(_.contains(i))
      .collect { case w if w.nonEmpty => (w.head, w.last) }
      .getOrElse((start, end))
}
object BQIntegerRange {
  def default: BQIntegerRange = BQIntegerRange(start = 0, end = 4000, interval = 1)
}
