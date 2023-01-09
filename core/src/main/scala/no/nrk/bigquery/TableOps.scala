package no.nrk.bigquery

import cats.effect.IO

import java.time.{LocalDate, YearMonth}
import scala.annotation.implicitNotFound

@implicitNotFound(
  "You have lost the precise type of the table, it needs to be something like `LocalDate`, but is ${P}. You can pattern match to recover this type information. If you wanted the list of partitions, check out `loadGenericPartitions`."
)
trait TableOps[P] {
  def assertPartition(table: BQTableLike[P], partition: P): BQPartitionId[P]

  def loadPartitions(
      table: BQTableLike[P],
      client: BigQueryClient,
      startDate: StartDate[P],
      requireRowNums: Boolean
  ): IO[Vector[(BQPartitionId[P], PartitionMetadata)]]
}

object TableOps {
  def apply[P: TableOps]: TableOps[P] = implicitly

  implicit val date: TableOps[LocalDate] = new TableOps[LocalDate] {
    override def assertPartition(
        table: BQTableLike[LocalDate],
        partition: LocalDate
    ): BQPartitionId[LocalDate] =
      table.partitionType match {
        case BQPartitionType.DatePartitioned(_) =>
          BQPartitionId.DatePartitioned(table, partition)
        case _: BQPartitionType.Sharded =>
          BQPartitionId.Sharded(table, partition)
      }

    override def loadPartitions(
        table: BQTableLike[LocalDate],
        client: BigQueryClient,
        startDate: StartDate[LocalDate],
        requireRowNums: Boolean
    ): IO[Vector[(BQPartitionId[LocalDate], PartitionMetadata)]] =
      table.partitionType match {
        case x: BQPartitionType.DatePartitioned =>
          PartitionLoader.date(
            table,
            x.field,
            client,
            startDate,
            requireRowNums
          )
        case _: BQPartitionType.Sharded =>
          PartitionLoader.shard(table, client, startDate)
      }
  }

  implicit val month: TableOps[YearMonth] = new TableOps[YearMonth] {
    override def assertPartition(
        table: BQTableLike[YearMonth],
        partition: YearMonth
    ): BQPartitionId[YearMonth] =
      BQPartitionId.MonthPartitioned(table, partition)

    override def loadPartitions(
        table: BQTableLike[YearMonth],
        client: BigQueryClient,
        startDate: StartDate[YearMonth],
        requireRowNums: Boolean
    ): IO[Vector[(BQPartitionId[YearMonth], PartitionMetadata)]] =
      table.partitionType match {
        case x: BQPartitionType.MonthPartitioned =>
          PartitionLoader.month(
            table,
            x.field,
            client,
            startDate,
            requireRowNums
          )
      }
  }

  implicit val unit: TableOps[Unit] = new TableOps[Unit] {
    override def assertPartition(
        table: BQTableLike[Unit],
        partition: Unit
    ): BQPartitionId[Unit] =
      BQPartitionId.NotPartitioned(table)

    override def loadPartitions(
        table: BQTableLike[Unit],
        client: BigQueryClient,
        startDate: StartDate[Unit],
        requireRowNums: Boolean
    ): IO[Vector[(BQPartitionId[Unit], PartitionMetadata)]] =
      table.partitionType match {
        case _: BQPartitionType.NotPartitioned =>
          PartitionLoader.unpartitioned(table, client).map(Vector(_))
      }
  }
}
