/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package internal

import com.google.cloud.bigquery.{
  MaterializedViewDefinition,
  RangePartitioning,
  StandardTableDefinition,
  TimePartitioning
}

object PartitionTypeHelper {
  def timePartitioned(bqtype: BQPartitionType[Any], tableOptions: TableOptions): Option[TimePartitioning] =
    bqtype match {
      case BQPartitionType.DatePartitioned(field) =>
        Some(
          TimePartitioning
            .newBuilder(TimePartitioning.Type.DAY)
            .setExpirationMs(tableOptions.partitionExpiration.map(exp => Long.box(exp.toMillis)).orNull)
            .setField(field.value)
            .build()
        )

      case BQPartitionType.MonthPartitioned(field) =>
        Some(
          TimePartitioning
            .newBuilder(TimePartitioning.Type.MONTH)
            .setExpirationMs(tableOptions.partitionExpiration.map(exp => Long.box(exp.toMillis)).orNull)
            .setField(field.value)
            .build()
        )
      case _: BQPartitionType.Sharded => None
      case _: BQPartitionType.NotPartitioned => None
      case _: BQPartitionType.RangePartitioned => None
    }

  def rangepartitioned(bqtype: BQPartitionType[Any]): Option[RangePartitioning] =
    bqtype match {
      case BQPartitionType.RangePartitioned(field, range) =>
        Some(
          RangePartitioning
            .newBuilder()
            .setRange(
              RangePartitioning.Range
                .newBuilder()
                .setStart(range.start)
                .setEnd(range.end)
                .setInterval(range.interval)
                .build())
            .setField(field.value)
            .build()
        )
      case _ => None
    }

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
