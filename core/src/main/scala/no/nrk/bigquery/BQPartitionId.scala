/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import no.nrk.bigquery.syntax._
import cats.Show

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, YearMonth}

/** A reference to a partition within a table.
  *
  * An irritation with modern SQL for BQ is that it is only possible to always refer to a partition of a *sharded*
  * table.
  *
  * For (date or otherwise) partitioned tables we haven't found a good way yet.
  *
  * For `SELECT` the best way so far is to use a subquery which specifies partition (`asSubQuery`), but this doesn't
  * syntactically work with for instance `DELETE`
  *
  * For inserts the Java SDK allows us to specify the partition in a `TableId` structure, so `asTableId` can be used
  */
sealed trait BQPartitionId[+P] {
  val partition: P
  val wholeTable: BQTableLike[P]
  def asTableId: BQTableId
  def asSubQuery: BQSqlFrag

  /** This is a compromise. Originally `BQPartitionId` was parametrized by LocalDate, Unit and so on. In order to
    * simplify a bit we settled on this form as that value rendered to `String`. It can be used to compare dates for
    * `BQPartitionId`s across different tables, for instance
    */
  def partitionString: String
  final override def toString: String = asTableId.asString
}

object BQPartitionId {
  val localDateNoDash: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyyMMdd")

  val yearMonthNoDash: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyyMM")

  // newest dates first for all
  implicit def ordering[Pid <: BQPartitionId[Any]]: Ordering[Pid] =
    Ordering.by((x: Pid) => (x.partitionString, x.wholeTable.toString)).reverse

  implicit def shows[Pid <: BQPartitionId[Any]]: Show[Pid] =
    pid => pid.asTableId.asString

  final case class DatePartitioned(
      wholeTable: BQTableLike[LocalDate],
      partition: LocalDate
  ) extends BQPartitionId[LocalDate] {
    def field: Ident = wholeTable.partitionType match {
      case BQPartitionType.DatePartitioned(field) => field
      case other => sys.error(s"Unexpected $other")
    }

    def asSubQuery: BQSqlFrag =
      bqfr"""(select * from ${wholeTable.tableId.asFragment} where $field = $partition)"""

    def asTableId: BQTableId =
      wholeTable.tableId.modifyTableName(_ + "$" + partitionString)

    override def partitionString: String =
      partition.format(localDateNoDash)
  }

  final case class MonthPartitioned(
      wholeTable: BQTableLike[YearMonth],
      partition: YearMonth
  ) extends BQPartitionId[YearMonth] {
    def field: Ident = wholeTable.partitionType match {
      case BQPartitionType.MonthPartitioned(field) => field
      case other => sys.error(s"Unexpected $other")
    }

    def asSubQuery: BQSqlFrag =
      bqfr"""(select * from ${wholeTable.tableId.asFragment} where $field = $partition)"""

    def asTableId: BQTableId =
      wholeTable.tableId.modifyTableName(_ + "$" + partitionString)

    override def partitionString: String =
      partition.format(yearMonthNoDash)
  }

  final case class Sharded(
      wholeTable: BQTableLike[LocalDate],
      partition: LocalDate
  ) extends BQPartitionId[LocalDate] {
    require(
      !wholeTable.tableId.tableName.endsWith("_"),
      s"we no longer use `_` suffix for sharded table names. Found in $wholeTable"
    )

    override def asTableId: BQTableId =
      wholeTable.tableId.modifyTableName(_ + "_" + partitionString)

    override def asSubQuery: BQSqlFrag =
      bqsql"(select * from ${asTableId.asFragment})"

    override def partitionString: String =
      partition.format(localDateNoDash)
  }

  final case class NotPartitioned(wholeTable: BQTableLike[Unit]) extends BQPartitionId[Unit] {
    override val partition: Unit = ()

    override def asSubQuery: BQSqlFrag =
      bqsql"(select * from ${asTableId.asFragment})"

    override def asTableId: BQTableId =
      wholeTable.tableId

    override def partitionString: String =
      ""
  }
}
