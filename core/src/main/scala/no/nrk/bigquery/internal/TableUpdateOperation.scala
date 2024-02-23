/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package internal

import cats.Eq
import cats.syntax.all.*

object TableUpdateOperation {
  private implicit val partitionTypeEquality: Eq[BQPartitionType[Any]] = Eq.instance((a, b) => a == b)

  private implicit val tableDefEquality: Eq[BQTableDef.Table[Any]] = Eq.instance { (a, b) =>
    a.tableId == b.tableId &&
    a.schema == b.schema &&
    a.partitionType == b.partitionType &&
    a.clustering == b.clustering &&
    a.description == b.description &&
    a.labels == b.labels &&
    a.tableOptions == b.tableOptions
  }

  private implicit val viewDefEquality: Eq[BQTableDef.View[Any]] = Eq.instance { (a, b) =>
    a.tableId == b.tableId &&
    a.schema == b.schema &&
    a.partitionType === b.partitionType &&
    a.query.asString == b.query.asString &&
    a.description == b.description &&
    a.labels == b.labels
  }

  private implicit val materializedViewDefEquality: Eq[BQTableDef.MaterializedView[Any]] = Eq.instance { (a, b) =>
    a.tableId == b.tableId &&
    a.schema == b.schema &&
    a.partitionType === b.partitionType &&
    a.query.asString == b.query.asString &&
    a.description == b.description &&
    a.labels == b.labels &&
    a.enableRefresh == b.enableRefresh &&
    a.refreshIntervalMs == b.refreshIntervalMs &&
    a.tableOptions == b.tableOptions
  }

  def from(
      tableDef: BQTableDef[Any],
      maybeExisting: Option[ExistingTable]
  ): UpdateOperation =
    maybeExisting match {
      case None =>
        tableDef match {
          case t: BQTableDef.Table[?] =>
            UpdateOperation.CreateTable(t, None)
          case view: BQTableDef.View[?] =>
            UpdateOperation.CreateTable(view.copy(schema = BQSchema.of()), Some(view))
          case matView: BQTableDef.MaterializedView[?] =>
            UpdateOperation.CreateTable(matView, None)
        }

      case Some(existingDef) =>
        (tableDef -> existingDef.our) match {
          case (local: BQTableDef.Table[Any], remote: BQTableDef.Table[Any])
              if remote.partitionType === local.partitionType =>
            val illegal = conforms
              .onlyTypes(
                actualSchema = remote.schema,
                givenSchema = local.schema
              )
              .map { reasons =>
                UpdateOperation.IllegalSchemaExtension(
                  TableDefOperationMeta(remote, local),
                  reasons.mkString(", ")
                )
              }

            if (local === remote)
              UpdateOperation.Noop(TableDefOperationMeta(remote, local))
            else
              illegal.getOrElse {
                UpdateOperation.UpdateTable(existingDef, local)
              }

          case (local: BQTableDef.Table[Any], remote: BQTableDef.Table[Any]) =>
            UpdateOperation.UnsupportedPartitioning(
              TableDefOperationMeta(remote, local),
              s"Cannot change partitioning from ${remote.partitionType} to ${local.partitionType}"
            )
          case (local: BQTableDef.View[Any], remote: BQTableDef.View[Any]) =>
            if (local === remote.withTableType(local.partitionType))
              UpdateOperation.Noop(TableDefOperationMeta(remote, local))
            else
              UpdateOperation.RecreateView(
                existingDef,
                local,
                UpdateOperation.CreateTable(local.copy(schema = BQSchema.of()), Some(local)))

          case (local: BQTableDef.MaterializedView[Any], remote: BQTableDef.MaterializedView[Any])
              if local.partitionType === remote.partitionType =>
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

            if (patchedLocalMVDef === remote)
              UpdateOperation.Noop(TableDefOperationMeta(remote, local))
            else
              UpdateOperation.RecreateView(
                existingDef,
                local,
                UpdateOperation.CreateTable(local.copy(schema = BQSchema.of()), Some(patchedLocalMVDef))
              )

          case (local: BQTableDef.MaterializedView[Any], remote: BQTableDef.MaterializedView[Any]) =>
            val reason =
              s"Cannot change partitioning from ${remote.partitionType} to ${local.partitionType}"
            UpdateOperation.UnsupportedPartitioning(TableDefOperationMeta(remote, local), reason)
          case (_, _) =>
            val reason =
              s"cannot update a ${existingDef.getClass.getSimpleName} to ${tableDef.getClass.getSimpleName}"
            UpdateOperation.Illegal(TableDefOperationMeta(existingDef.our, tableDef), reason)
        }
    }
}
