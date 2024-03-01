/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package client.google.internal

import com.google.cloud.bigquery.{Option as _, *}
import no.nrk.bigquery.*

import scala.jdk.CollectionConverters.*
import GoogleTypeHelper.*

object TableHelper {
  sealed trait TableConversionError
  object TableConversionError {
    final case class IllegalTableId(msg: String) extends TableConversionError
    final case class UnsupportedPartitionType(msg: String) extends TableConversionError
    final case class UnsupportedTableType(msg: String) extends TableConversionError
  }

  def fromGoogle(table: TableInfo): Either[TableConversionError, BQTableDef[Any]] = {
    val id = List(
      Option(table.getTableId.getProject),
      Option(table.getTableId.getDataset),
      Option(table.getTableId.getTable)).flatten.mkString(".")
    val parsedId = BQTableId.fromString(id).left.map(msg => TableConversionError.IllegalTableId(msg))

    table.getDefinition[TableDefinition] match {
      case st: StandardTableDefinition =>
        for {
          typ <- PartitionTypeHelper.from(st).left.map(msg => TableConversionError.UnsupportedPartitionType(msg))
          tableId <- parsedId
        } yield BQTableDef.Table(
          tableId = tableId,
          schema = SchemaHelper.fromSchema(st.getSchema),
          partitionType = typ,
          description = Option(table.getDescription),
          clustering = Option(st.getClustering).toList
            .flatMap(_.getFields.asScala)
            .map(Ident.apply),
          labels = GoogleTypeHelper.tableLabelsfromTableInfo(table),
          tableOptions = GoogleTypeHelper.toTableOptions(table)
        )

      case v: ViewDefinition =>
        parsedId.map(tableId =>
          BQTableDef.View(
            tableId = tableId,
            schema = SchemaHelper.fromSchema(v.getSchema),
            partitionType = BQPartitionType.NotPartitioned,
            description = Option(table.getDescription),
            labels = GoogleTypeHelper.tableLabelsfromTableInfo(table),
            query = BQSqlFrag.Frag(v.getQuery)
          ))

      case v: MaterializedViewDefinition =>
        for {
          typ <- PartitionTypeHelper.from(v).left.map(msg => TableConversionError.UnsupportedPartitionType(msg))
          tableId <- parsedId

        } yield BQTableDef.MaterializedView(
          tableId = tableId,
          schema = SchemaHelper.fromSchema(v.getSchema),
          partitionType = typ,
          description = Option(table.getDescription),
          enableRefresh = Option(v.getEnableRefresh).forall(_.booleanValue()),
          refreshIntervalMs = Option(v.getRefreshIntervalMs).map(_.longValue()).getOrElse(1800000L),
          /*
          TODO: Add clustering to BQTableDef.MaterializedView

          clustering = Option(st.getClustering).toList
                .flatMap(_.getFields.asScala)
                .map(Ident.apply),*/
          labels = GoogleTypeHelper.tableLabelsfromTableInfo(table),
          query = BQSqlFrag.Frag(v.getQuery),
          tableOptions = GoogleTypeHelper.toTableOptions(table)
        )

      case s => Left(TableConversionError.UnsupportedTableType(s"${s.getType.name()} is not supported here"))
    }
  }

  def toGoogle(
      tableDef: BQTableDef[Any],
      maybeExisting: Option[TableInfo]
  ): TableInfo =
    maybeExisting match {
      case None =>
        createNew(tableDef)

      case Some(existingRemoteTable) =>
        tableDef match {
          case local: BQTableDef.Table[Any] =>
            updateTable(existingRemoteTable, local)

          case local: BQTableDef.ViewLike[Any] =>
            val recreate = createNew(local)
            val updatedLabels =
              labelsForUpdate(local.labels, Some(GoogleTypeHelper.tableLabelsfromTableInfo(existingRemoteTable)))
            recreate.toBuilder.setLabels(updatedLabels).build()
        }
    }

  private def updateTable(existingRemoteTable: TableInfo, local: BQTableDef.Table[Any]) = {
    val existingLabels = GoogleTypeHelper.tableLabelsfromTableInfo(existingRemoteTable)
    // unpack values from `localTableDef`. This will break compilation we add more fields, reminding us to update here
    val BQTableDef.Table(
      _,
      schema,
      partitioning,
      description,
      clustering,
      labels,
      tableOptions
    ) = local

    existingRemoteTable.toBuilder
      .setDefinition {
        StandardTableDefinition.newBuilder
          .setSchema(SchemaHelper.toSchema(schema))
          .setTimePartitioning(
            PartitionTypeHelper.timePartitioned(partitioning, tableOptions).orNull
          )
          .setRangePartitioning(
            PartitionTypeHelper.rangepartitioned(partitioning).orNull
          )
          .setClustering(clusteringFrom(clustering).orNull)
          .build()
      }
      .setRequirePartitionFilter(
        // Override partitionFilterRequired flag if the table is not partitioned.
        partitioning match {
          case BQPartitionType.DatePartitioned(_) => tableOptions.partitionFilterRequired
          case BQPartitionType.MonthPartitioned(_) => tableOptions.partitionFilterRequired
          case _ => null
        }
      )
      .setDescription(description.orNull)
      .setLabels(labelsForUpdate(labels, Some(existingLabels)))
      .build()
  }

  def createNew(localTableDef: BQTableDef[Any]): TableInfo =
    localTableDef match {
      // Views must first be created without schema, then updated with schema. (2021-01-13)
      case BQTableDef.View(tableId, _, query, schema, description, labels) =>
        val withoutSchema: TableInfo =
          newTable(
            tableId.underlying,
            ViewDefinition.of(query.asStringWithUDFs),
            TableOptions.Empty,
            description,
            labels
          )

        val withSchema: TableInfo =
          withoutSchema.toBuilder
            .setDefinition(
              withoutSchema
                .getDefinition[ViewDefinition]
                .toBuilder
                .setSchema(SchemaHelper.toSchema(schema))
                .build()
            )
            .build()

        if (schema.fields.isEmpty) withoutSchema else withSchema

      case BQTableDef.Table(
            tableId,
            schema,
            partitionType,
            description,
            clustering,
            labels,
            tableOptions
          ) =>
        val toCreate: TableInfo =
          newTable(
            tableId.underlying,
            StandardTableDefinition.newBuilder
              .setSchema(SchemaHelper.toSchema(schema))
              .setTimePartitioning(PartitionTypeHelper.timePartitioned(partitionType, tableOptions).orNull)
              .setRangePartitioning(PartitionTypeHelper.rangepartitioned(partitionType).orNull)
              .setClustering(clusteringFrom(clustering).orNull)
              .build,
            tableOptions,
            description,
            labels
          )
        toCreate

      case BQTableDef.MaterializedView(
            tableId,
            partitionType,
            query,
            schema @ _,
            enableRefresh,
            refreshIntervalMs,
            description,
            labels,
            tableOptions
          ) =>
        val toCreate: TableInfo =
          newTable(
            tableId.underlying,
            MaterializedViewDefinition
              .newBuilder(query.asStringWithUDFs)
              .setEnableRefresh(enableRefresh)
              // .setSchema(schema.toSchema)  // not possible for now
              .setRefreshIntervalMs(refreshIntervalMs)
              .setTimePartitioning(PartitionTypeHelper.timePartitioned(partitionType, tableOptions).orNull)
              .setRangePartitioning(PartitionTypeHelper.rangepartitioned(partitionType).orNull)
              .build(),
            tableOptions,
            description,
            labels
          )

        toCreate
    }

  /** This method is needed for the case where we delete a label. It is deleted by setting it to `null`.
    *
    * As such, we need to know the labels of a table in production before we compute the new set of labels to use when
    * updating
    */
  def labelsForUpdate(labels: TableLabels, existingLabels: Option[TableLabels]): java.util.Map[String, String] = {
    val ret = new java.util.TreeMap[String, String]

    // existing table? set all old label keys to `null`
    for {
      exLabels <- existingLabels.toList
      existingKey <- exLabels.values.keySet
    } ret.put(existingKey, null)

    // and overwrite the ones we want to keep.
    labels.values.foreach { case (key, value) => ret.put(key, value) }

    ret
  }

  private def newTable(
      tableId: TableId,
      definition: TableDefinition,
      tableOptions: TableOptions,
      description: Option[String],
      labels: TableLabels
  ): TableInfo = {
    val builder = TableInfo
      .newBuilder(tableId, definition)
      .setDescription(description.orNull)
      .setLabels(labelsForUpdate(labels, existingLabels = None))

    // For some reason requirePartitionFilter = false <> requirePartitionFilter = null
    if (tableOptions.partitionFilterRequired) {
      builder.setRequirePartitionFilter(tableOptions.partitionFilterRequired).build()
    } else {
      builder.build()
    }
  }

  private def clusteringFrom(clustering: List[Ident]): Option[Clustering] =
    clustering match {
      case Nil => None
      case nonEmpty =>
        Some(
          Clustering
            .newBuilder()
            .setFields(nonEmpty.map(_.value).asJava)
            .build()
        )
    }
}
