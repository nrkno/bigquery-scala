/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import java.time.{LocalDate, LocalDateTime, YearMonth}

sealed trait StartPartition[+T] {
  def asMonth: StartPartition[YearMonth] =
    this match {
      case x: StartPartition.FromDate => StartPartition.FromMonth(YearMonth.from(x.startInclusive))
      case x: StartPartition.FromDateTime => StartPartition.FromMonth(YearMonth.from(x.startInclusive.toLocalDate))
      case x: StartPartition.FromMonth => x
      case _ => StartPartition.All
    }

  def asDate: StartPartition[LocalDate] =
    this match {
      case x: StartPartition.FromDate => x
      case x: StartPartition.FromMonth => StartPartition.FromDate(x.startInclusive.atDay(1))
      case x: StartPartition.FromDateTime => StartPartition.FromDate(x.startInclusive.toLocalDate)
      case _ => StartPartition.All
    }

  def asDateTime: StartPartition[LocalDateTime] =
    this match {
      case x: StartPartition.FromDateTime => x
      case x: StartPartition.FromMonth => StartPartition.FromDateTime(x.startInclusive.atDay(1).atStartOfDay)
      case _ => StartPartition.All
    }

  def asRange: StartPartition[Long] =
    this match {
      case x: StartPartition.FromRangeValue => x
      case _ => StartPartition.All
    }

}

object StartPartition {
  case object All extends StartPartition[Nothing]
  case class FromDateTime(startInclusive: LocalDateTime) extends StartPartition[LocalDateTime]
  case class FromDate(startInclusive: LocalDate) extends StartPartition[LocalDate]
  case class FromMonth(startInclusive: YearMonth) extends StartPartition[YearMonth]
  case class FromRangeValue(startInclusive: Long) extends StartPartition[Long]
}
