/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package client.http4s.internal

import cats.syntax.all.*
import googleapis.bigquery.*

import scala.collection.immutable.SortedMap

object TableHelper {
  def toTableReference(tableId: BQTableId) =
    TableReference(Some(tableId.dataset.project.value), Some(tableId.dataset.id), Some(tableId.tableName))

  def fromTableReference(ref: TableReference) =
    (ref.projectId, ref.datasetId, ref.tableId).mapN((p, d, t) => s"$p.$d.$t").flatMap(BQTableId.fromString(_).toOption)

  def fromGoogle(table: Table): Either[String, BQTableDef[Any]] =
    for {
      ref <- table.tableReference.flatMap(fromTableReference).toRight("No table reference provided")
      table <- table.`type` match {
        case Some("TABLE") | None => toTable(table, ref)
        case Some("VIEW") => toView(table, ref)
        case Some("MATERIALIZED_VIEW") => toMaterializedView(table, ref)
        case x => Left(s"'${x.getOrElse("UNKNOWN")}' is not supported")
      }
    } yield table

  def refFromGoogle(table: TableListTable): Either[String, BQTableRef[Any]] =
    for {
      ref <- table.tableReference.flatMap(fromTableReference).toRight("No table reference provided")
      convert = Table(timePartitioning = table.timePartitioning, rangePartitioning = table.rangePartitioning)
      partitionType <- extractPartitionFrom(convert)
    } yield BQTableRef(
      tableId = ref,
      partitionType = partitionType,
      labels = TableLabels(SortedMap.from(table.labels.getOrElse(Map.empty[String, String])))
    )

  def fromPartition(partitionType: BQPartitionType[Any], tableOptions: TableOptions) =
    partitionType match {
      case BQPartitionType.HourPartitioned(field) =>
        Some(
          TimePartitioning(
            expirationMs = tableOptions.partitionExpiration,
            field = Some(field.value),
            requirePartitionFilter = Some(tableOptions.partitionFilterRequired),
            `type` = Some("HOUR")
          )) -> None
      case BQPartitionType.DatePartitioned(field) =>
        Some(
          TimePartitioning(
            expirationMs = tableOptions.partitionExpiration,
            field = Some(field.value),
            requirePartitionFilter = Some(tableOptions.partitionFilterRequired),
            `type` = Some("DAY")
          )) -> None
      case BQPartitionType.MonthPartitioned(field) =>
        Some(
          TimePartitioning(
            expirationMs = tableOptions.partitionExpiration,
            field = Some(field.value),
            requirePartitionFilter = Some(tableOptions.partitionFilterRequired),
            `type` = Some("MONTH")
          )) -> None
      case BQPartitionType.IntegerRangePartitioned(field, range) =>
        (
          None,
          Some(
            RangePartitioning(
              Some(field.value),
              Some(
                RangePartitioningRange(
                  end = Some(range.end),
                  interval = Some(range.interval),
                  start = Some(range.start))))
          ))
      case _ => (None, None)
    }

  def toGoogle(table: BQTableDef[Any], existing: Option[Table]) = {
    val t = existing.getOrElse(Table(tableReference = Some(toTableReference(table.tableId))))
    val existingTableLabels = t.labels.getOrElse(Map.empty)
    val updatedLabels = existingTableLabels ++ table.labels.values

    table match {
      case BQTableDef.Table(_, schema, partitionType, description, clustering, _, tableOptions) =>
        val (time, range) = fromPartition(partitionType, tableOptions)

        val updated = time
          .map(tm => t.copy(timePartitioning = Some(tm)))
          .orElse(range.map(r => t.copy(rangePartitioning = Some(r))))
          .getOrElse(t)

        updated.copy(
          description = description,
          clustering = Option.when(clustering.nonEmpty)(Clustering(Some(clustering.map(_.value)))),
          schema = Some(SchemaHelper.toTableSchema(schema)),
          labels = Some(existingTableLabels ++ updatedLabels)
        )

      case BQTableDef.View(_, _, query, schema, description, _) =>
        t.copy(
          description = description,
          view = Some(ViewDefinition(None, Some(false), Some(query.asString), None, None)),
          schema = Option.when(schema.fields.nonEmpty)(SchemaHelper.toTableSchema(schema)),
          labels = Some(updatedLabels)
        )

      case BQTableDef.MaterializedView(
            _,
            partitionType,
            query,
            schema,
            enableRefresh,
            refreshIntervalMs,
            description,
            _,
            tableOptions) =>
        val (time, range) = fromPartition(partitionType, tableOptions)

        val updated = time
          .map(tm => t.copy(timePartitioning = Some(tm)))
          .orElse(range.map(r => t.copy(rangePartitioning = Some(r))))
          .getOrElse(t)

        updated.copy(
          description = description,
          materializedView = Some(value = MaterializedViewDefinition(
            query = Some(query.asString),
            enableRefresh = Some(enableRefresh),
            refreshIntervalMs = Some(refreshIntervalMs))),
          schema = Option.when(schema.fields.nonEmpty)(SchemaHelper.toTableSchema(schema)),
          labels = Some(updatedLabels)
        )
    }

  }

  def toPartition(range: RangePartitioning): Either[String, Option[BQPartitionType[Any]]] =
    (range.range, range.field.map(Ident.apply)).tupled match {
      case None => Right(None)
      case Some((RangePartitioningRange(Some(end), Some(interval), Some(start)), field)) =>
        Right(Some(BQPartitionType.IntegerRangePartitioned(field, BQIntegerRange(start, end, interval))))
      case Some((x, field)) => Left(s"Unsupported '$x' partitioning on field(${field.value})")
    }

  def toPartition(time: TimePartitioning): Either[String, Option[BQPartitionType[Any]]] =
    (time.`type`, time.field.map(Ident.apply)).tupled match {
      case None => Right(None)
      case Some(("DAY", field)) => Right(Some(BQPartitionType.DatePartitioned(field)))
      case Some(("HOUR", field)) => Right(Some(BQPartitionType.HourPartitioned(field)))
      case Some(("MONTH", field)) => Right(Some(BQPartitionType.MonthPartitioned(field)))
      case Some((x, field)) => Left(s"Unsupported '$x' partitioning on field(${field.value})")
    }

  def extractPartitionFrom(table: Table): Either[String, BQPartitionType[Any]] =
    table.timePartitioning.map(toPartition).orElse(table.rangePartitioning.map(toPartition)) match {
      case Some(value) =>
        value.flatMap {
          case None => "Unable to determine partitionType".asLeft[BQPartitionType[Any]]
          case Some(x) => x.asRight[String]
        }
      case None => Right(BQPartitionType.NotPartitioned)
    }

  private def toTable(table: Table, id: BQTableId) =
    for {
      schema <- table.schema.map(SchemaHelper.fromTableSchema).toRight("No schema provided")
      partitionType <- extractPartitionFrom(table)
    } yield BQTableDef.Table(
      id,
      schema,
      partitionType,
      table.description,
      table.clustering.flatMap(_.fields).getOrElse(Nil).map(Ident.apply),
      labels = TableLabels.apply(SortedMap.from(table.labels.getOrElse(Map.empty))),
      tableOptions =
        TableOptions(table.requirePartitionFilter.getOrElse(false), table.timePartitioning.flatMap(_.expirationMs))
    )
  private def toView(table: Table, id: BQTableId) =
    for {
      schema <- table.schema.map(SchemaHelper.fromTableSchema).toRight("No schema provided")
      partitionType = BQPartitionType.NotPartitioned
    } yield BQTableDef.View(
      tableId = id,
      partitionType = partitionType,
      schema = schema,
      query = table.view.flatMap(_.query).map(BQSqlFrag.Frag.apply).getOrElse(BQSqlFrag.Empty),
      description = table.description,
      labels = TableLabels.apply(SortedMap.from(table.labels.getOrElse(Map.empty)))
    )

  private def toMaterializedView(table: Table, id: BQTableId) =
    for {
      schema <- table.schema.map(SchemaHelper.fromTableSchema).toRight("No schema provided")
      partitionType = BQPartitionType.NotPartitioned
    } yield BQTableDef.MaterializedView(
      tableId = id,
      partitionType = partitionType,
      query = table.materializedView.flatMap(_.query).map(BQSqlFrag.Frag.apply).getOrElse(BQSqlFrag.Empty),
      schema = schema,
      description = table.description,
      refreshIntervalMs = table.materializedView.flatMap(_.refreshIntervalMs).getOrElse(1800000L),
      enableRefresh = table.materializedView.flatMap(_.enableRefresh).getOrElse(true),
      labels = TableLabels.apply(SortedMap.from(table.labels.getOrElse(Map.empty))),
      tableOptions =
        TableOptions(table.requirePartitionFilter.getOrElse(false), table.timePartitioning.flatMap(_.expirationMs))
    )
}
