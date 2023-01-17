package no.nrk.bigquery

import cats.effect.Concurrent
import cats.syntax.all._

import no.nrk.bigquery.implicits._
import com.google.cloud.bigquery.TableId
import io.circe._
import io.circe.syntax._

/** @tparam P
  *   partition specifier. typically [[java.time.LocalDate]] or [[scala.Unit]]
  */
sealed trait BQTableLike[+P] {
  val tableId: TableId
  val partitionType: BQPartitionType[P]
  def withTableType[PP](tpe: BQPartitionType[PP]): BQTableLike[PP]
  def unpartitioned: BQTableLike[Unit]
}

object BQTableLike {
  implicit def encodes[P]: Encoder[BQTableLike[P]] =
    Encoder.instance[BQTableLike[P]] {
      case x: BQTableRef[_] =>
        Json.obj(
          "tableId" := x.tableId,
          "partitionType" := x.partitionType,
          "type" := "BQTableRef"
        )
      case x: BQTableDef[p] => BQTableDef.encodes[p](x)
    }

  implicit class ExtensionMethods(private val table: BQTableLike[Any])
      extends AnyVal {
    def loadGenericPartitions[F[_]: Concurrent](
        client: BigQueryClient[F],
        startDate: StartDate[Any],
        requireRowNums: Boolean = false
    ): F[Vector[(BQPartitionId[Any], PartitionMetadata)]] =
      PartitionLoader.loadGenericPartitions(
        table,
        client,
        startDate,
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
        startDate: StartDate[P],
        requireRowNums: Boolean = false
    )(implicit
        P: TableOps[P],
        C: Concurrent[F]
    ): F[Vector[(BQPartitionId[P], PartitionMetadata)]] =
      P.loadPartitions(table, client, startDate, requireRowNums)
  }

  implicit class ConditionallyAddedExtensionMethodsUnit(
      private val table: BQTableLike[Unit]
  ) extends AnyVal {
    def assertPartition: BQPartitionId[Unit] =
      TableOps.unit.assertPartition(table, ())

    def loadPartition[F[_]: Concurrent](
        client: BigQueryClient[F]
    ): F[(BQPartitionId[Unit], PartitionMetadata)] =
      PartitionLoader.unpartitioned(table, client).widen
  }
}

/** A reference to a table we know is partitioned in one of several ways.
  *
  * Given such a reference you can basically do two things:
  *
  * 1) Look up all partitions through `allPartitions`. This works with any
  * `BQPartitionedTable`. 2) Construct a legal [[BQPartitionId]] for it with
  * `assertPartition`.
  *
  * The design of this tri-structure was a bit problematic, especially regarding
  * variance.
  *
  * This iteration of the design uses covariant type parameters throughout, with
  * the caveat that when you combine (say) a non-partitioned and a
  * date-partitioned table into a list (or otherwise lose precise types), you
  * also lose the ability to construct legal [[BQPartitionId]] s with
  * `assertPartition`
  */
case class BQTableRef[+P](tableId: TableId, partitionType: BQPartitionType[P])
    extends BQTableLike[P] {
  override def unpartitioned: BQTableRef[Unit] =
    withTableType(BQPartitionType.ignoredPartitioning(partitionType))

  override def withTableType[PP](tpe: BQPartitionType[PP]): BQTableRef[PP] =
    BQTableRef(tableId, tpe)
}

/** Our version of a description of what a BQ table/view should look like.
  */
sealed trait BQTableDef[+P] extends BQTableLike[P] {
  val tableId: TableId
  val description: Option[String]
  val schema: BQSchema
  val labels: TableLabels

  labels.verify(tableId)
}

object BQTableDef {
  implicit def encodes[P]: Encoder[BQTableDef[P]] =
    Encoder.instance[BQTableDef[P]] {
      case x: BQTableDef.Table[_] =>
        Json.obj(
          "tableId" := x.tableId,
          "schema" := x.schema,
          "partitionType" := x.partitionType,
          "description" := x.description,
          "clustering" := x.clustering,
          "labels" := x.labels,
          "type" := "Table"
        )

      case x: BQTableDef.View[_] =>
        Json.obj(
          "tableId" := x.tableId,
          "partitionType" := x.partitionType,
          "query" := x.query,
          "schema" := x.schema,
          "description" := x.description,
          "labels" := x.labels,
          "type" := "View"
        )

      case x: BQTableDef.MaterializedView[_] =>
        Json.obj(
          "tableId" := x.tableId,
          "partitionType" := x.partitionType,
          "query" := x.query,
          "schema" := x.schema,
          "enableRefresh" := x.enableRefresh,
          "refreshIntervalMs" := x.refreshIntervalMs,
          "description" := x.description,
          "labels" := x.labels,
          "type" := "MaterializedView"
        )
    }

  /** @param schema
    *   note that BQ will only allow backward compatible schema changes, you
    *   cannot remove fields for instance
    * @param partitionType
    *   it's very unlikely that this can be changed, but at least we'll fail if
    *   you try
    * @param description
    *   table description
    */
  case class Table[+P](
      tableId: TableId,
      schema: BQSchema,
      partitionType: BQPartitionType[P],
      description: Option[String] = None,
      clustering: List[Ident] = Nil,
      labels: TableLabels = TableLabels.Empty
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
      tableId: TableId,
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

  /** Note that none of these fields can be changed after the MV has been
    * created.
    *
    * Need to change anything? Create a new one with a (bumped) version suffix
    *
    * @param schema
    *   *NOT* used for creating the MV because it's not possible yet. However,
    *   we use it to test that `query` conforms, we can use it for docs and so
    *   on.
    */
  case class MaterializedView[+P](
      tableId: TableId,
      partitionType: BQPartitionType[P],
      query: BQSqlFrag,
      schema: BQSchema,
      enableRefresh: Boolean = true, /* default from BQ */
      refreshIntervalMs: Long = 1800000, /* 30 minutes, default from BQ */
      description: Option[String] = None,
      labels: TableLabels = TableLabels.Empty
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
