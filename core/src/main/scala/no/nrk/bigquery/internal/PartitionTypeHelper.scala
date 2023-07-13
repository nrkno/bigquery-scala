package no.nrk.bigquery
package internal

import com.google.cloud.bigquery.{
  MaterializedViewDefinition,
  RangePartitioning,
  StandardTableDefinition,
  TimePartitioning
}

object PartitionTypeHelper {
  def timePartitioned(bqtype: BQPartitionType[Any]) =
    bqtype match {
      case BQPartitionType.DatePartitioned(field) =>
        Some(
          TimePartitioning
            .newBuilder(TimePartitioning.Type.DAY)
            .setField(field.value)
            .build()
        )

      case BQPartitionType.MonthPartitioned(field) =>
        Some(
          TimePartitioning
            .newBuilder(TimePartitioning.Type.MONTH)
            .setField(field.value)
            .build()
        )
      case _: BQPartitionType.Sharded => None
      case _: BQPartitionType.NotPartitioned => None
    }

  def rangepartitioned(bqtype: BQPartitionType[Any]): Option[RangePartitioning] =
    None

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
