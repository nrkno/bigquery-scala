/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import no.nrk.bigquery.syntax.*
import cats.effect.Concurrent
import cats.syntax.all.*
import no.nrk.bigquery.util.{Nat, Sized}

/** @tparam P
  *   partition specifier. typically [[java.time.LocalDate]] or [[scala.Unit]]
  */
sealed trait BQTableLike[+P] {
  def tableId: BQTableId
  def partitionType: BQPartitionType[P]
  def withTableType[PP](tpe: BQPartitionType[PP]): BQTableLike[PP]
  def unpartitioned: BQTableLike[Unit]
  def wholeTable: WholeTable[P] = WholeTable(this)
  def asFragment: BQSqlFrag
}

object BQTableLike {
  implicit class ExtensionMethods(private val table: BQTableLike[Any]) extends AnyVal {
    def loadGenericPartitions[F[_]: Concurrent](
        client: BigQueryClient[F],
        startPartition: StartPartition[Any],
        requireRowNums: Boolean = false
    ): F[Vector[(BQPartitionId[Any], PartitionMetadata)]] =
      PartitionLoader.loadGenericPartitions(
        table,
        client,
        startPartition,
        requireRowNums
      )
  }

  implicit class ConditionallyAddedExtensionMethods[P](
      private val table: BQTableLike[P]
  ) extends AnyVal {
    def assertPartition(partition: P)(implicit
        P: TableOps[P]
    ): BQPartitionId[P] =
      P.assertPartition(table, partition)

    def loadPartitions[F[_]](
        client: BigQueryClient[F],
        startPartition: StartPartition[P],
        requireRowNums: Boolean = false
    )(implicit
        P: TableOps[P],
        C: Concurrent[F]
    ): F[Vector[(BQPartitionId[P], PartitionMetadata)]] =
      P.loadPartitions(table, client, startPartition, requireRowNums)
  }

  implicit class ConditionallyAddedExtensionMethodsUnit(
      private val table: BQTableLike[Unit]
  ) extends AnyVal {
    def assertPartition: BQPartitionId[Unit] =
      TableOps.unit.assertPartition(table, ())

    def loadPartition[F[_]: Concurrent](
        client: BigQueryClient[F]
    ): F[Option[(BQPartitionId[Unit], PartitionMetadata)]] =
      PartitionLoader.unpartitioned(table, client).widen
  }
}

/** A reference to a table we know is partitioned in one of several ways.
  *
  * Given such a reference you can basically do two things:
  *
  * 1) Look up all partitions through `allPartitions`. This works with any `BQPartitionedTable`. 2) Construct a legal
  * [[BQPartitionId]] for it with `assertPartition`.
  *
  * The design of this tri-structure was a bit problematic, especially regarding variance.
  *
  * This iteration of the design uses covariant type parameters throughout, with the caveat that when you combine (say)
  * a non-partitioned and a date-partitioned table into a list (or otherwise lose precise types), you also lose the
  * ability to construct legal [[BQPartitionId]] s with `assertPartition`
  */
case class BQTableRef[+P](
    tableId: BQTableId,
    partitionType: BQPartitionType[P],
    labels: TableLabels = TableLabels.Empty)
    extends BQTableLike[P] {
  override def unpartitioned: BQTableRef[Unit] =
    withTableType(BQPartitionType.ignoredPartitioning(partitionType))

  override def withTableType[PP](tpe: BQPartitionType[PP]): BQTableRef[PP] =
    BQTableRef(tableId, tpe)

  def asFragment: BQSqlFrag = tableId.asFragment
}

case class BQAppliedTableValuedFunction[+P](
    name: TVF.TVFId,
    // this doesn't exist physically, only in a sense when querying
    partitionType: BQPartitionType[P],
    params: List[BQRoutine.Param],
    query: BQSqlFrag,
    schema: BQSchema,
    description: Option[String] = None,
    args: List[BQSqlFrag]
) extends BQTableLike[P] {
  override def tableId: BQTableId =
    BQTableId(name.dataset, name.name.value)

  override def unpartitioned: BQTableLike[Unit] =
    withTableType(BQPartitionType.ignoredPartitioning(partitionType))

  override def withTableType[PP](tpe: BQPartitionType[PP]): BQTableLike[PP] =
    BQAppliedTableValuedFunction(name, tpe, params, query, schema, description, args)

  def asFragment: BQSqlFrag = tableId.asFragment ++ args.mkFragment("(", ", ", ")")
}

object BQAppliedTableValuedFunction {
  def apply[P, N <: Nat](
      tvf: TVF[P, N],
      args: Sized[IndexedSeq[BQSqlFrag], N]
  ): BQAppliedTableValuedFunction[P] =
    BQAppliedTableValuedFunction(
      tvf.name,
      tvf.partitionType,
      tvf.params.unsized.toList,
      tvf.query,
      tvf.schema,
      tvf.description,
      args.unsized.toList
    )
}

/** Our version of a description of what a BQ table/view should look like.
  */
sealed trait BQTableDef[+P] extends BQTableLike[P] {
  def tableId: BQTableId
  def description: Option[String]
  def schema: BQSchema
  def labels: TableLabels
  def asFragment: BQSqlFrag = tableId.asFragment
}

case class WholeTable[+P](table: BQTableLike[P])

object BQTableDef {

  /** @param schema
    *   note that BQ will only allow backward compatible schema changes, you cannot remove fields for instance
    * @param partitionType
    *   it's very unlikely that this can be changed, but at least we'll fail if you try
    * @param description
    *   table description
    */
  case class Table[+P](
      tableId: BQTableId,
      schema: BQSchema,
      partitionType: BQPartitionType[P],
      description: Option[String] = None,
      clustering: List[Ident] = Nil,
      labels: TableLabels = TableLabels.Empty,
      tableOptions: TableOptions = TableOptions.Empty
  ) extends BQTableDef[P] {
    def unpartitioned: Table[Unit] =
      withTableType(BQPartitionType.ignoredPartitioning(partitionType))

    override def withTableType[NewParam](
        tpe: BQPartitionType[NewParam]
    ): Table[NewParam] =
      Table(tableId, schema, tpe, description, clustering, labels)
  }

  sealed trait ViewLike[+P] extends BQTableDef[P] {
    val query: BQSqlFrag
  }

  case class View[+P](
      tableId: BQTableId,
      // this doesn't exist physically, only in a sense when querying
      partitionType: BQPartitionType[P],
      query: BQSqlFrag,
      schema: BQSchema,
      description: Option[String] = None,
      labels: TableLabels = TableLabels.Empty
  ) extends ViewLike[P] {
    override def unpartitioned: View[Unit] =
      withTableType(BQPartitionType.ignoredPartitioning(partitionType))

    override def withTableType[NewParam](
        tpe: BQPartitionType[NewParam]
    ): View[NewParam] =
      View(tableId, tpe, query, schema, description, labels)
  }

  /** Note that none of these fields can be changed after the MV has been created.
    *
    * Need to change anything? Create a new one with a (bumped) version suffix
    *
    * @param schema
    *   *NOT* used for creating the MV because it's not possible yet. However, we use it to test that `query` conforms,
    *   we can use it for docs and so on.
    */
  case class MaterializedView[+P](
      tableId: BQTableId,
      partitionType: BQPartitionType[P],
      query: BQSqlFrag,
      schema: BQSchema,
      enableRefresh: Boolean = true, /* default from BQ */
      refreshIntervalMs: Long = 1800000, /* 30 minutes, default from BQ */
      description: Option[String] = None,
      labels: TableLabels = TableLabels.Empty,
      tableOptions: TableOptions = TableOptions.Empty
  ) extends ViewLike[P] {
    override def unpartitioned: MaterializedView[Unit] =
      withTableType(BQPartitionType.ignoredPartitioning(partitionType))

    override def withTableType[PP](
        tpe: BQPartitionType[PP]
    ): MaterializedView[PP] =
      MaterializedView(
        tableId,
        tpe,
        query,
        schema,
        enableRefresh,
        refreshIntervalMs,
        description,
        labels
      )
  }
}
