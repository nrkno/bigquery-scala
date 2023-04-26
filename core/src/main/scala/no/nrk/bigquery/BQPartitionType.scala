package no.nrk.bigquery

import cats.syntax.all._
import com.google.cloud.bigquery.{
  MaterializedViewDefinition,
  RangePartitioning,
  StandardTableDefinition,
  TimePartitioning
}

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
  def timePartitioning: Option[TimePartitioning]
  def rangePartitioning: Option[RangePartitioning]
}

object BQPartitionType {

  final case class DatePartitioned(field: Ident) extends BQPartitionType[LocalDate] {
    override def timePartitioning: Option[TimePartitioning] =
      Some(
        TimePartitioning
          .newBuilder(TimePartitioning.Type.DAY)
          .setField(field.show)
          .build()
      )

    override def rangePartitioning: Option[RangePartitioning] = None
  }

  final case class MonthPartitioned(field: Ident) extends BQPartitionType[YearMonth] {
    override def timePartitioning: Option[TimePartitioning] =
      Some(
        TimePartitioning
          .newBuilder(TimePartitioning.Type.MONTH)
          .setField(field.show)
          .build()
      )

    override def rangePartitioning: Option[RangePartitioning] = None
  }

  sealed trait Sharded extends BQPartitionType[LocalDate] {
    override def timePartitioning: Option[TimePartitioning] = None
    override def rangePartitioning: Option[RangePartitioning] = None
  }

  // note: the reason why there are both a sealed trait and an object with the same name is to say `Sharded` sharded instead of `Sharded.type`.
  case object Sharded extends Sharded

  sealed trait NotPartitioned extends BQPartitionType[Unit] {
    override def timePartitioning: Option[TimePartitioning] = None
    override def rangePartitioning: Option[RangePartitioning] = None
  }

  case object NotPartitioned extends NotPartitioned

  def ignoredPartitioning[P](original: BQPartitionType[P]): NotPartitioned =
    original match {
      case x: NotPartitioned => x
      case other => IgnoredPartitioning(other)
    }

  /* this only exists to recover information in tests, for instance when staging view queries for date-partitioned fills */
  case class IgnoredPartitioning[P](original: BQPartitionType[P]) extends NotPartitioned

  def from(
      actual: StandardTableDefinition
  ): Either[String, BQPartitionType[Any]] =
    from(
      Option(actual.getTimePartitioning),
      Option(actual.getRangePartitioning)
    )

  def from(
      actual: MaterializedViewDefinition
  ): Either[String, BQPartitionType[Any]] =
    from(
      Option(actual.getTimePartitioning),
      Option(actual.getRangePartitioning)
    )

  def from(
      timePartitioning: Option[TimePartitioning],
      rangePartitioning: Option[RangePartitioning]
  ): Either[String, BQPartitionType[Any]] =
    (timePartitioning, rangePartitioning) match {
      case (None, None) =>
        Right(BQPartitionType.NotPartitioned)
      case (Some(time), None) if time.getType == TimePartitioning.Type.DAY && time.getField != null =>
        Right(BQPartitionType.DatePartitioned(Ident(time.getField)))
      case (Some(time), None) if time.getType == TimePartitioning.Type.MONTH && time.getField != null =>
        Right(BQPartitionType.MonthPartitioned(Ident(time.getField)))
      case (time, range) =>
        Left(
          s"Need to implement support in `BQPartitionType` for ${time.orElse(range)}"
        )
    }
}
