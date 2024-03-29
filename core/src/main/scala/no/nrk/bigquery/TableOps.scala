/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.effect.Concurrent
import cats.syntax.all.*

import java.time.{LocalDate, LocalDateTime, YearMonth}
import scala.annotation.implicitNotFound

@implicitNotFound(
  "You have lost the precise type of the table, it needs to be something like `LocalDate`, but is ${P}. You can pattern match to recover this type information. If you wanted the list of partitions, check out `loadGenericPartitions`."
)
trait TableOps[P] {
  def assertPartition(table: BQTableLike[P], partition: P): BQPartitionId[P]

  def loadPartitions[F[_]: Concurrent](
      table: BQTableLike[P],
      client: QueryClient[F],
      startPartition: StartPartition[P],
      requireRowNums: Boolean
  ): F[Vector[(BQPartitionId[P], PartitionMetadata)]]
}

object TableOps {
  def apply[P: TableOps]: TableOps[P] = implicitly

  implicit val hour: TableOps[LocalDateTime] = new TableOps[LocalDateTime] {
    override def assertPartition(
        table: BQTableLike[LocalDateTime],
        partition: LocalDateTime
    ): BQPartitionId[LocalDateTime] =
      table.partitionType match {
        case BQPartitionType.HourPartitioned(_) =>
          BQPartitionId.HourPartitioned(table, partition)
      }

    override def loadPartitions[F[_]: Concurrent](
        table: BQTableLike[LocalDateTime],
        client: QueryClient[F],
        startPartition: StartPartition[LocalDateTime],
        requireRowNums: Boolean
    ): F[Vector[(BQPartitionId[LocalDateTime], PartitionMetadata)]] =
      table.partitionType match {
        case x: BQPartitionType.HourPartitioned =>
          PartitionLoader
            .hour(
              table,
              x.field,
              client,
              startPartition,
              requireRowNums
            )
            .widen
      }
  }

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

    override def loadPartitions[F[_]: Concurrent](
        table: BQTableLike[LocalDate],
        client: QueryClient[F],
        startPartition: StartPartition[LocalDate],
        requireRowNums: Boolean
    ): F[Vector[(BQPartitionId[LocalDate], PartitionMetadata)]] =
      table.partitionType match {
        case x: BQPartitionType.DatePartitioned =>
          PartitionLoader
            .date(
              table,
              x.field,
              client,
              startPartition,
              requireRowNums
            )
            .widen
        case _: BQPartitionType.Sharded =>
          PartitionLoader.shard(table, client, startPartition).widen
      }
  }

  implicit val month: TableOps[YearMonth] = new TableOps[YearMonth] {
    override def assertPartition(
        table: BQTableLike[YearMonth],
        partition: YearMonth
    ): BQPartitionId[YearMonth] =
      BQPartitionId.MonthPartitioned(table, partition)

    override def loadPartitions[F[_]: Concurrent](
        table: BQTableLike[YearMonth],
        client: QueryClient[F],
        startPartition: StartPartition[YearMonth],
        requireRowNums: Boolean
    ): F[Vector[(BQPartitionId[YearMonth], PartitionMetadata)]] =
      table.partitionType match {
        case x: BQPartitionType.MonthPartitioned =>
          PartitionLoader
            .month(
              table,
              x.field,
              client,
              startPartition,
              requireRowNums
            )
            .widen
      }
  }

  implicit val range: TableOps[Long] = new TableOps[Long] {
    override def assertPartition(
        table: BQTableLike[Long],
        partition: Long
    ): BQPartitionId[Long] =
      table.partitionType match {
        case _: BQPartitionType.IntegerRangePartitioned =>
          BQPartitionId.IntegerRangePartitioned(table, partition)
      }

    override def loadPartitions[F[_]: Concurrent](
        table: BQTableLike[Long],
        client: QueryClient[F],
        startPartition: StartPartition[Long],
        requireRowNums: Boolean
    ): F[Vector[(BQPartitionId[Long], PartitionMetadata)]] =
      table.partitionType match {
        case x: BQPartitionType.IntegerRangePartitioned =>
          PartitionLoader
            .range(
              table,
              x.field,
              client,
              startPartition,
              requireRowNums
            )
            .widen
      }
  }

  implicit val unit: TableOps[Unit] = new TableOps[Unit] {
    override def assertPartition(
        table: BQTableLike[Unit],
        partition: Unit
    ): BQPartitionId[Unit] =
      BQPartitionId.NotPartitioned(table)

    override def loadPartitions[F[_]: Concurrent](
        table: BQTableLike[Unit],
        client: QueryClient[F],
        startPartition: StartPartition[Unit],
        requireRowNums: Boolean
    ): F[Vector[(BQPartitionId[Unit], PartitionMetadata)]] =
      table.partitionType match {
        case _: BQPartitionType.NotPartitioned =>
          PartitionLoader.unpartitioned(table, client).map(_.toVector).widen
      }
  }
}
