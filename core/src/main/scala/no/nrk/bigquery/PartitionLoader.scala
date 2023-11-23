/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.effect.Concurrent
import cats.syntax.all._
import no.nrk.bigquery.syntax._
import fs2.Stream

import java.time.{Instant, LocalDate, YearMonth}
import scala.annotation.nowarn

private[bigquery] object PartitionLoader {
  def loadGenericPartitions[F[_]: Concurrent](
      table: BQTableLike[Any],
      client: BigQueryClient[F],
      startPartition: StartPartition[Any],
      requireRowNums: Boolean = false
  ): F[Vector[(BQPartitionId[Any], PartitionMetadata)]] =
    table.partitionType match {
      case x: BQPartitionType.DatePartitioned =>
        PartitionLoader
          .date(
            table.withTableType[LocalDate](x),
            x.field,
            client,
            startPartition.asDate,
            requireRowNums
          )
          .widen
      case x: BQPartitionType.MonthPartitioned =>
        PartitionLoader
          .month(
            table.withTableType[YearMonth](x),
            x.field,
            client,
            startPartition.asMonth,
            requireRowNums
          )
          .widen
      case sharded: BQPartitionType.Sharded =>
        PartitionLoader
          .shard(
            table.withTableType[LocalDate](sharded),
            client,
            startPartition.asDate
          )
          .widen

      case x: BQPartitionType.RangePartitioned =>
        PartitionLoader
          .range(
            table.withTableType[Long](x),
            x.field,
            client,
            startPartition.asRange,
            requireRowNums
          )
          .widen
      case notPartitioned: BQPartitionType.NotPartitioned =>
        PartitionLoader
          .unpartitioned(table.withTableType[Unit](notPartitioned), client)
          .map(_.toVector)
    }

  case class LongInstant(value: Instant)
  object LongInstant {
    implicit val bqReads: BQRead[LongInstant] =
      BQRead.convertsLong.map(long => LongInstant(Instant.ofEpochMilli(long)))
  }

  def localDateFromPartitionName(tableName: String): LocalDate =
    LocalDate.parse(tableName.takeRight(8), BQPartitionId.localDateNoDash)

  object date {
    def apply[F[_]](
        table: BQTableLike[LocalDate],
        field: Ident,
        client: BigQueryClient[F],
        startPartition: StartPartition[LocalDate],
        requireRowNums: Boolean
    )(implicit
        F: Concurrent[F]
    ): F[Vector[(BQPartitionId.DatePartitioned, PartitionMetadata)]] = {
      val rowNumByDate: F[Map[LocalDate, Long]] =
        if (requireRowNums)
          client
            .synchronousQuery(
              BQJobId.auto,
              rowCountQuery(table, field, startPartition)
            )
            .compile
            .to(Map)
        else F.pure(Map.empty)

      // views do not have metadata we can ask with partitions, so fire an actual query to get the data
      val rowsIO: Stream[F, (LocalDate, Option[Instant], Option[Instant])] =
        table match {
          case view: BQTableDef.View[LocalDate] =>
            client
              .synchronousQuery(
                BQJobId.auto,
                allPartitionsQueries
                  .fromTableData[LocalDate](view.unpartitioned, field)
              )
              .map(partitionDate => (partitionDate, None, None))
          case _ =>
            client
              .synchronousQuery(
                BQJobId.auto,
                allPartitionsQuery(table, startPartition),
                legacySql = true
              )
              .map { case (partitionName, creationTime, lastModifiedTime) =>
                (
                  localDateFromPartitionName(partitionName),
                  Some(creationTime.value),
                  Some(lastModifiedTime.value)
                )
              }
        }

      for {
        rows <- rowsIO.compile.toVector
        rowNumByDate <- rowNumByDate
      } yield rows.map { case (date, l1, l2) =>
        BQPartitionId.DatePartitioned(table, date) -> PartitionMetadata(
          l1,
          l2,
          rowCount = rowNumByDate.get(date),
          None
        )
      }
    }

    def rowCountQuery(
        table: BQTableLike[LocalDate],
        field: Ident,
        startPartition: StartPartition[LocalDate]
    ): BQQuery[(LocalDate, Long)] = {
      val inRange = startPartition match {
        case StartPartition.All => bqfr"true"
        case StartPartition.FromDate(startInclusive) =>
          bqfr"$field >= $startInclusive"
      }
      allPartitionsQueries.withRowCountFromTableData(
        table.unpartitioned,
        inRange,
        field
      )
    }

    def allPartitionsQuery(
        table: BQTableLike[LocalDate],
        startPartition: StartPartition[LocalDate]
    ): BQQuery[(String, LongInstant, LongInstant)] = {
      val inRange = startPartition match {
        case StartPartition.All => bqfr"true"
        case StartPartition.FromDate(startInclusive) =>
          bqfr"x.partition_id >= ${StringValue(startInclusive.format(BQPartitionId.localDateNoDash))}"
      }

      allPartitionsQueries.fromMetadata(table, inRange)
    }
  }

  object month {
    def apply[F[_]](
        table: BQTableLike[YearMonth],
        field: Ident,
        client: BigQueryClient[F],
        start: StartPartition[YearMonth],
        requireRowNums: Boolean
    )(implicit
        F: Concurrent[F]
    ): F[Vector[(BQPartitionId.MonthPartitioned, PartitionMetadata)]] = {
      val rowNumByDate: F[Map[YearMonth, Long]] =
        if (requireRowNums)
          client
            .synchronousQuery(
              BQJobId.auto,
              rowCountQuery(table, field, start)
            )
            .compile
            .to(Map)
        else F.pure(Map.empty)

      // views do not have metadata we can ask with partitions, so fire an actual query to get the data
      val rowsIO: Stream[F, (YearMonth, Option[Instant], Option[Instant])] =
        table match {
          case view: BQTableDef.View[YearMonth] =>
            val query = allPartitionsQueries
              .fromTableData[YearMonth](view.unpartitioned, field)
            client
              .synchronousQuery(BQJobId.auto, query)
              .map(partitionDate => (partitionDate, None, None))
          case _ =>
            client
              .synchronousQuery(
                BQJobId.auto,
                allPartitionsQuery(table, start),
                legacySql = true
              )
              .map { case (partitionName, creationTime, lastModifiedTime) =>
                val yearMonth = YearMonth.parse(
                  partitionName.takeRight(6),
                  BQPartitionId.yearMonthNoDash
                )
                (
                  yearMonth,
                  Some(creationTime.value),
                  Some(lastModifiedTime.value)
                )
              }
        }

      for {
        rows <- rowsIO.compile.toVector
        rowNumByMonth <- rowNumByDate
      } yield rows.map { case (month, l1, l2) =>
        BQPartitionId.MonthPartitioned(table, month) -> PartitionMetadata(
          l1,
          l2,
          rowCount = rowNumByMonth.get(month),
          None
        )
      }
    }

    def allPartitionsQuery(
        table: BQTableLike[YearMonth],
        start: StartPartition[YearMonth]
    ): BQQuery[(String, LongInstant, LongInstant)] = {
      val inRange = start match {
        case StartPartition.All => bqfr"true"
        case StartPartition.FromMonth(startInclusive) =>
          bqfr"x.partition_id >= ${StringValue(startInclusive.format(BQPartitionId.yearMonthNoDash))}"
      }
      allPartitionsQueries.fromMetadata(table, inRange)
    }

    def rowCountQuery(
        table: BQTableLike[YearMonth],
        field: Ident,
        start: StartPartition[YearMonth]
    ): BQQuery[(YearMonth, Long)] = {
      val inRange = start match {
        case StartPartition.All => bqfr"true"
        case StartPartition.FromMonth(startInclusive) =>
          bqfr"$field >= $startInclusive"
      }

      allPartitionsQueries.withRowCountFromTableData(
        table.unpartitioned,
        inRange,
        field
      )
    }
  }

  object shard {
    def apply[F[_]](
        table: BQTableLike[LocalDate],
        client: BigQueryClient[F],
        startPartition: StartPartition[LocalDate]
    )(implicit
        F: Concurrent[F]
    ): F[Vector[(BQPartitionId.Sharded, PartitionMetadata)]] =
      client
        .synchronousQuery(
          BQJobId.auto,
          allPartitionsQuery(startPartition, table),
          legacySql = true
        )
        .map { case (partitionName, l1, l2, l3, l4) =>
          val metadata = PartitionMetadata(
            Some(l1.value),
            Some(l2.value),
            Some(l3),
            Some(l4)
          )
          BQPartitionId.Sharded(
            table,
            localDateFromPartitionName(partitionName)
          ) -> metadata
        }
        .compile
        .toVector

    def allPartitionsQuery(
        startPartition: StartPartition[LocalDate],
        table: BQTableLike[LocalDate]
    ): BQQuery[(String, LongInstant, LongInstant, Long, Long)] = {
      val inRange = startPartition match {
        case StartPartition.All => bqfr"true"
        case StartPartition.FromDate(startInclusive) =>
          bqfr"table_id >= ${StringValue(BQPartitionId.Sharded(table, startInclusive).asTableId.tableName)}"
      }

      BQQuery {
        bqfr"""|SELECT table_id, creation_time, last_modified_time, row_count, size_bytes
               |FROM [${BQSqlFrag(
            table.tableId.dataset.project.value
          )}:${BQSqlFrag(
            table.tableId.dataset.id
          )}.__TABLES__]
               |WHERE REGEXP_MATCH(table_id, r"${BQSqlFrag(
            table.tableId.tableName
          )}_[0-9]+")
               |AND $inRange
               |ORDER BY 1 DESC""".stripMargin
      }(BQRead.derived)
    }
  }

  object range {
    def apply[F[_]](
        table: BQTableLike[Long],
        field: Ident,
        client: BigQueryClient[F],
        startPartition: StartPartition[Long],
        requireRowNums: Boolean
    )(implicit
        F: Concurrent[F]
    ): F[Vector[(BQPartitionId.RangePartitioned, PartitionMetadata)]] = {
      val rowNumByInt: F[Map[Long, Long]] =
        if (requireRowNums)
          client
            .synchronousQuery(
              BQJobName.auto,
              rowCountQuery(table, field, startPartition)
            )
            .compile
            .to(Map)
        else F.pure(Map.empty)

      // views do not have metadata we can ask with partitions, so fire an actual query to get the data
      val rowsIO: Stream[F, (Long, Option[Instant], Option[Instant])] =
        table match {
          case view: BQTableDef.View[Long] =>
            client
              .synchronousQuery(
                BQJobName.auto,
                allPartitionsQueries
                  .fromTableData[Long](view.unpartitioned, field)
              )
              .map(partition => (partition, None, None))
          case _ =>
            client
              .synchronousQuery(
                BQJobName.auto,
                allPartitionsQuery(table, startPartition),
                legacySql = true
              )
              .map { case (partitionName, creationTime, lastModifiedTime) =>
                (
                  partitionName.toLong,
                  Some(creationTime.value),
                  Some(lastModifiedTime.value)
                )
              }
        }

      for {
        rows <- rowsIO.compile.toVector
        rowsNumByInt <- rowNumByInt
      } yield rows.map { case (partition, l1, l2) =>
        BQPartitionId.RangePartitioned(table, partition) -> PartitionMetadata(
          l1,
          l2,
          rowCount = rowsNumByInt.get(partition),
          None
        )
      }
    }

    def rowCountQuery(
        table: BQTableLike[Long],
        field: Ident,
        startPartition: StartPartition[Long]
    ): BQQuery[(Long, Long)] = {
      val inRange = startPartition match {
        case StartPartition.All => bqfr"true"
        case StartPartition.FromRangeValue(startInclusive) => bqfr"$field >= $startInclusive"
      }
      allPartitionsQueries.withRowCountFromTableData(
        table.unpartitioned,
        inRange,
        field
      )
    }

    def allPartitionsQuery(
        table: BQTableLike[Long],
        startPartition: StartPartition[Long]
    ): BQQuery[(String, LongInstant, LongInstant)] = {
      val inRange = startPartition match {
        case StartPartition.All => bqfr"true"
        case StartPartition.FromRangeValue(startInclusive) =>
          bqfr"x.partition_id >= ${StringValue(startInclusive.toString)}"
      }

      allPartitionsQueries.fromMetadata(table, inRange)
    }
  }

  object unpartitioned {
    def apply[F[_]](
        table: BQTableLike[Unit],
        client: BigQueryClient[F]
    )(implicit
        F: Concurrent[F]
    ): F[Option[(BQPartitionId.NotPartitioned, PartitionMetadata)]] =
      client
        .synchronousQuery(
          BQJobId.auto,
          partitionQuery(table.tableId),
          legacySql = true
        )
        .map { case (creationTime, lastModifiedTime, rowCount, sizeBytes) =>
          val partition = BQPartitionId.NotPartitioned(table)
          val metadata = PartitionMetadata(
            creationTime.map(_.value),
            lastModifiedTime.map(_.value),
            rowCount,
            sizeBytes
          )
          (partition, metadata)
        }
        .compile
        .last

    def partitionQuery(tableId: BQTableId): BQQuery[
      (Option[LongInstant], Option[LongInstant], Option[Long], Option[Long])
    ] = {
      val query =
        bqfr"""
      SELECT creation_time, last_modified_time, row_count, size_bytes
      FROM [${BQSqlFrag(tableId.dataset.project.value)}:${BQSqlFrag(
            tableId.dataset.id
          )}.__TABLES__]
      WHERE table_id = ${StringValue(tableId.tableName)}"""

      BQQuery(query)(BQRead.derived)
    }
  }

  // these queries should be possible to reuse for all partitioning schemes except sharded and unpartitioned
  object allPartitionsQueries {
    // this costs nothing, the other queries will have a minimal cost
    def fromMetadata(
        table: BQTableLike[Any],
        inRange: BQSqlFrag
    ): BQQuery[(String, LongInstant, LongInstant)] = {
      // legacy sql table reference with a table decorator to ask for all partitions. this syntax is not available in standard sql
      val partitionsSummary =
        bqfr"[${BQSqlFrag(table.tableId.dataset.project.value)}.${BQSqlFrag(
            table.tableId.dataset.id
          )}.${BQSqlFrag(table.tableId.tableName)}$$__PARTITIONS_SUMMARY__]"

      BQQuery {
        bqfr"""|SELECT x.partition_id, x.creation_time, x.last_modified_time
               |FROM $partitionsSummary x
               |where x.partition_id != '__UNPARTITIONED__'
               |and x.partition_id != '__NULL__'
               |and ${inRange}
               |ORDER BY 1 DESC""".stripMargin
      }(BQRead.derived)
    }

    def fromTableData[P: BQRead](
        table: BQTableLike[Unit],
        partitionColumn: Ident
    ): BQQuery[P] =
      BQQuery {
        bqfr"""|SELECT DISTINCT $partitionColumn
               |FROM $table
               |ORDER BY 1 DESC""".stripMargin
      }

    @nowarn
    def withRowCountFromTableData[P: BQRead](
        table: BQTableLike[Unit],
        inRange: BQSqlFrag,
        partitionColumn: Ident
    ): BQQuery[(P, Long)] =
      BQQuery {
        bqsql"""|select $partitionColumn, count(*) from $table
                |where $inRange
                |group by $partitionColumn
                |order by 1""".stripMargin
      }(BQRead.derived)
  }
}
