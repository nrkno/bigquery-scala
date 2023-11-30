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
  def fieldOpt: Option[Ident]
}

object BQPartitionType {

  final case class DatePartitioned(field: Ident) extends BQPartitionType[LocalDate] {
    override def fieldOpt: Option[Ident] = Option(field)
  }

  final case class MonthPartitioned(field: Ident) extends BQPartitionType[YearMonth] {
    override def fieldOpt: Option[Ident] = Option(field)
  }

  final case class RangePartitioned(field: Ident, range: BQRange = BQRange.default) extends BQPartitionType[Long] {
    override def fieldOpt: Option[Ident] = Option(field)
  }

  sealed trait Sharded extends BQPartitionType[LocalDate]

  // note: the reason why there are both a sealed trait and an object with the same name is to say `Sharded` sharded instead of `Sharded.type`.
  case object Sharded extends Sharded {
    override def fieldOpt: Option[Ident] = None
  }

  sealed trait NotPartitioned extends BQPartitionType[Unit]

  case object NotPartitioned extends NotPartitioned {
    override def fieldOpt: Option[Ident] = None
  }

  def ignoredPartitioning[P](original: BQPartitionType[P]): NotPartitioned =
    original match {
      case x: NotPartitioned => x
      case other => IgnoredPartitioning(other)
    }

  /* this only exists to recover information in tests, for instance when staging view queries for date-partitioned fills */
  case class IgnoredPartitioning[P](original: BQPartitionType[P]) extends NotPartitioned {
    override def fieldOpt: Option[Ident] = original.fieldOpt
  }
}
