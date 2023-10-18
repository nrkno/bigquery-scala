/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.internal

import com.google.cloud.bigquery.{Option => _, _}
import no.nrk.bigquery._

import scala.jdk.CollectionConverters._
import GoogleTypeHelper._

object TableUpdateOperation {

  def from(
      tableDef: BQTableDef[Any],
      maybeExisting: Option[TableInfo]
  ): UpdateOperation =
    maybeExisting match {
      case None =>
        createNew(tableDef)

      case Some(existingRemoteTable) =>
        SchemaHelper.fromTable(existingRemoteTable) match {
          case Left(SchemaHelper.TableConversionError.UnsupportedPartitionType(msg)) =>
            UpdateOperation.UnsupportedPartitioning(TableDefOperationMeta(existingRemoteTable, tableDef), msg)
          case Left(SchemaHelper.TableConversionError.UnsupportedTableType(msg)) =>
            UpdateOperation.Illegal(TableDefOperationMeta(existingRemoteTable, tableDef), msg)
          case Left(SchemaHelper.TableConversionError.IllegalTableId(msg)) =>
            UpdateOperation.Illegal(TableDefOperationMeta(existingRemoteTable, tableDef), msg)
          case Right(remoteDef) =>
            (tableDef -> remoteDef) match {
              case (local: BQTableDef.Table[Any], remote: BQTableDef.Table[Any])
                  if remote.partitionType == local.partitionType =>
                val illegalSchemaExtension: Option[UpdateOperation.IllegalSchemaExtension] =
                  conforms
                    .onlyTypes(
                      actualSchema = remote.schema,
                      givenSchema = local.schema
                    )
                    .map { reasons =>
                      UpdateOperation.IllegalSchemaExtension(
                        TableDefOperationMeta(existingRemoteTable, tableDef),
                        reasons.mkString(", ")
                      )
                    }

                if (local == remote)
                  UpdateOperation.Noop(TableDefOperationMeta(existingRemoteTable, local))
                else
                  illegalSchemaExtension.getOrElse {
                    val updatedTable: TableInfo = updateTable(existingRemoteTable, local, remote)
                    UpdateOperation.UpdateTable(
                      existingRemoteTable,
                      local,
                      updatedTable
                    )
                  }
              case (local: BQTableDef.Table[Any], remote: BQTableDef.Table[Any]) =>
                UpdateOperation.UnsupportedPartitioning(
                  TableDefOperationMeta(existingRemoteTable, local),
                  s"Cannot change partitioning from ${remote.partitionType} to ${local.partitionType}"
                )
              case (local: BQTableDef.View[Any], remote: BQTableDef.View[Any]) =>
                if (local == remote.withTableType(local.partitionType))
                  UpdateOperation.Noop(TableDefOperationMeta(existingRemoteTable, local))
                else
                  UpdateOperation.RecreateView(
                    existingRemoteTable,
                    local,
                    createNew(local)
                  )

              case (local: BQTableDef.MaterializedView[Any], remote: BQTableDef.MaterializedView[Any])
                  if local.partitionType == remote.partitionType =>
                def outline(field: BQField): BQField =
                  field.copy(
                    mode = BQField.Mode.NULLABLE,
                    description = None,
                    subFields = field.subFields.map(outline)
                  )

                val patchedLocalMVDef =
                  local.copy(
                    // Materialized views are given a schema, but we cant affect it in any way.
                    schema = BQSchema(local.schema.fields.map(outline)),
                    // Override partitionFilterRequired flag if the view is not partitioned.
                    tableOptions = local.partitionType match {
                      case BQPartitionType.DatePartitioned(_) => local.tableOptions
                      case BQPartitionType.MonthPartitioned(_) => local.tableOptions
                      case _ => local.tableOptions.copy(partitionFilterRequired = false)
                    }
                  )

                if (patchedLocalMVDef == remote)
                  UpdateOperation.Noop(TableDefOperationMeta(existingRemoteTable, local))
                else
                  UpdateOperation.RecreateView(
                    existingRemoteTable,
                    local,
                    createNew(local)
                  )

              case (local: BQTableDef.MaterializedView[Any], remote: BQTableDef.MaterializedView[Any]) =>
                val reason =
                  s"Cannot change partitioning from ${remote.partitionType} to ${local.partitionType}"
                UpdateOperation.UnsupportedPartitioning(TableDefOperationMeta(existingRemoteTable, local), reason)
              case (_, _) =>
                val reason =
                  s"cannot update a ${existingRemoteTable.getDefinition[TableDefinition].getType.name()} to ${tableDef.getClass.getSimpleName}"
                UpdateOperation.Illegal(TableDefOperationMeta(existingRemoteTable, tableDef), reason)
            }
        }
    }

  private def updateTable(
      existingRemoteTable: TableInfo,
      local: BQTableDef.Table[Any],
      remote: BQTableDef.Table[Any]) = {
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
      .setLabels(labels.forUpdate(Some(remote)))
      .build()
  }

  def createNew(localTableDef: BQTableDef[Any]): UpdateOperation.CreateTable =
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

        UpdateOperation.CreateTable(localTableDef, withoutSchema, Some(withSchema))

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
        UpdateOperation.CreateTable(localTableDef, toCreate, None)

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

        UpdateOperation.CreateTable(localTableDef, toCreate, None)
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
      .setLabels(labels.forUpdate(maybeExistingTable = None))

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
