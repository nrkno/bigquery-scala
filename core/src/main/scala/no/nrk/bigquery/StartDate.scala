/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package no.nrk.bigquery

import java.time.{LocalDate, YearMonth}

sealed trait StartDate[+T] {
  def asMonth: StartDate[YearMonth] =
    this match {
      case StartDate.All => StartDate.All
      case x: StartDate.FromDate =>
        StartDate.FromMonth(YearMonth.from(x.startInclusive))
      case x: StartDate.FromMonth => x
    }

  def asDate: StartDate[LocalDate] =
    this match {
      case StartDate.All => StartDate.All
      case x: StartDate.FromDate => x
      case x: StartDate.FromMonth =>
        StartDate.FromDate(x.startInclusive.atDay(1))
    }
}

object StartDate {
  case object All extends StartDate[Nothing]
  case class FromDate(startInclusive: LocalDate) extends StartDate[LocalDate]
  case class FromMonth(startInclusive: YearMonth) extends StartDate[YearMonth]
}
