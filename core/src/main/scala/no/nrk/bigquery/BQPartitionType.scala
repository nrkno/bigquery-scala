/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import java.time.{LocalDate, YearMonth}

/** Describes a way a BQ table can be partitioned (or not).
  *
  * As as user you're expected to use this type to "tag" tables, but it doesn't expose much functionality directly.
  * Instead, it binds together several type and value mappings so that the rest of the machinery works.
  *
  * It exists as its own type because it's shared between [[BQTableRef]] and [[BQTableDef.Table]].
  *
  * It is parametrized by two types
  * @tparam Param
  *   A type which distinguishes one partition, typically [[java.time.LocalDate]]
  *
  * It is meant that when we add support for more partition schemes, the first step is to add a new subtype here.
  */
sealed trait BQPartitionType[+Param] {
  def partitionField: Option[Ident] = {
    val asType = this.asInstanceOf[BQPartitionType[Any]]
    asType match {
      case BQPartitionType.DatePartitioned(field) => Some(field)
      case BQPartitionType.MonthPartitioned(field) => Some(field)
      case BQPartitionType.IntegerRangePartitioned(field, _) => Some(field)
      case _: BQPartitionType.Sharded => None
      case _: BQPartitionType.NotPartitioned => None
    }
  }
}

object BQPartitionType {

  final case class DatePartitioned(field: Ident) extends BQPartitionType[LocalDate]
  final case class MonthPartitioned(field: Ident) extends BQPartitionType[YearMonth]
  final case class IntegerRangePartitioned(field: Ident, range: BQIntegerRange = BQIntegerRange.default)
      extends BQPartitionType[Long]

  sealed trait Sharded extends BQPartitionType[LocalDate]

  // note: the reason why there are both a sealed trait and an object with the same name is to say `Sharded` sharded instead of `Sharded.type`.
  case object Sharded extends Sharded

  sealed trait NotPartitioned extends BQPartitionType[Unit]

  case object NotPartitioned extends NotPartitioned

  def ignoredPartitioning[P](original: BQPartitionType[P]): NotPartitioned =
    original match {
      case x: NotPartitioned => x
      case other => IgnoredPartitioning(other)
    }

  /* this only exists to recover information in tests, for instance when staging view queries for date-partitioned fills */
  case class IgnoredPartitioning[P](original: BQPartitionType[P]) extends NotPartitioned
}
