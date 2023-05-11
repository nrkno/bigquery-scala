package no.nrk.bigquery.internal

import com.google.cloud.bigquery.{Option => _, _}
import no.nrk.bigquery._

import scala.jdk.CollectionConverters._

object TableUpdateOperation {

  def from(
      tableDef: BQTableDef[Any],
      maybeExisting: Option[TableInfo]
  ): UpdateOperation =
    maybeExisting match {
      case None =>
        createNew(tableDef)

      case Some(existingRemoteTable) =>
        (tableDef, existingRemoteTable.getDefinition[TableDefinition]) match {
          case (
                localTableDef: BQTableDef.Table[Any],
                remoteTableDef: StandardTableDefinition
              ) =>
            BQPartitionType.from(remoteTableDef) match {
              case Right(remotePartitionType) if remotePartitionType == localTableDef.partitionType =>
                val remoteAsTableDef = BQTableDef.Table(
                  tableId = BQTableId.unsafeFromGoogle(
                    localTableDef.tableId.dataset,
                    existingRemoteTable.getTableId
                  ),
                  schema = BQSchema.fromSchema(remoteTableDef.getSchema),
                  partitionType = remotePartitionType,
                  description = Option(existingRemoteTable.getDescription),
                  clustering = Option(remoteTableDef.getClustering).toList
                    .flatMap(_.getFields.asScala)
                    .map(Ident.apply),
                  labels = TableLabels.fromTableInfo(existingRemoteTable)
                )

                val illegalSchemaExtension: Option[UpdateOperation.IllegalSchemaExtension] =
                  conforms(
                    actualSchema = remoteAsTableDef.schema,
                    givenSchema = localTableDef.schema
                  ).map { reasons =>
                    UpdateOperation.IllegalSchemaExtension(
                      TableDefOperationMeta(existingRemoteTable, tableDef),
                      reasons.mkString(", ")
                    )
                  }

                if (localTableDef == remoteAsTableDef)
                  UpdateOperation.Noop(TableDefOperationMeta(existingRemoteTable, localTableDef))
                else
                  illegalSchemaExtension.getOrElse {
                    val updatedTable: TableInfo = {
                      // unpack values from `localTableDef`. This will break compilation we add more fields, reminding us to update here
                      val BQTableDef.Table(
                        _,
                        schema,
                        partitioning,
                        description,
                        clustering,
                        labels
                      ) = localTableDef

                      existingRemoteTable.toBuilder
                        .setDefinition {
                          StandardTableDefinition.newBuilder
                            .setSchema(schema.toSchema)
                            .setTimePartitioning(
                              partitioning.timePartitioning.orNull
                            )
                            .setRangePartitioning(
                              partitioning.rangePartitioning.orNull
                            )
                            .setClustering(clusteringFrom(clustering).orNull)
                            .build()
                        }
                        .setDescription(description.orNull)
                        .setLabels(labels.forUpdate(Some(remoteAsTableDef)))
                        .build()
                    }
                    UpdateOperation.UpdateTable(
                      existingRemoteTable,
                      localTableDef,
                      updatedTable
                    )
                  }
              case Right(wrongPartitionType) =>
                UpdateOperation.UnsupportedPartitioning(
                  TableDefOperationMeta(existingRemoteTable, localTableDef),
                  s"Cannot change partitioning from $wrongPartitionType to ${localTableDef.partitionType}"
                )
              case Left(unsupported) =>
                UpdateOperation.UnsupportedPartitioning(
                  TableDefOperationMeta(existingRemoteTable, localTableDef),
                  unsupported
                )
            }

          case (
                localTableDef: BQTableDef.View[Any],
                remoteViewDef: ViewDefinition
              ) =>
            val remoteAsTableDef = BQTableDef.View(
              tableId = tableDef.tableId,
              partitionType = localTableDef.partitionType,
              query = BQSqlFrag(remoteViewDef.getQuery),
              schema = BQSchema.fromSchema(remoteViewDef.getSchema),
              description = Option(existingRemoteTable.getDescription),
              labels = TableLabels.fromTableInfo(existingRemoteTable)
            )

            if (localTableDef == remoteAsTableDef)
              UpdateOperation.Noop(TableDefOperationMeta(existingRemoteTable, localTableDef))
            else
              UpdateOperation.RecreateView(
                existingRemoteTable,
                localTableDef,
                createNew(localTableDef)
              )

          case (
                localTemplate: BQTableDef.View[Any],
                _: MaterializedViewDefinition
              ) =>
            UpdateOperation.RecreateView(
              existingRemoteTable = existingRemoteTable,
              localTableDef = localTemplate,
              createNew(localTemplate)
            )

          case (
                localTableDef: BQTableDef.MaterializedView[Any],
                remoteMVDef: MaterializedViewDefinition
              ) =>
            BQPartitionType.from(remoteMVDef) match {
              case Right(remotePartitionType) if remotePartitionType == localTableDef.partitionType =>
                val remoteAsTableDef = BQTableDef.MaterializedView(
                  tableId = BQTableId.unsafeFromGoogle(
                    localTableDef.tableId.dataset,
                    existingRemoteTable.getTableId
                  ),
                  partitionType = remotePartitionType,
                  query = BQSqlFrag(remoteMVDef.getQuery),
                  schema = BQSchema.fromSchema(remoteMVDef.getSchema),
                  enableRefresh = remoteMVDef.getEnableRefresh,
                  refreshIntervalMs = remoteMVDef.getRefreshIntervalMs,
                  description = Option(existingRemoteTable.getDescription),
                  // note: we decided to not sync labels to BQ for MVs.
                  // this is because we have to recompute the whole thing on any label change.
                  labels = localTableDef.labels // TableLabels.fromTableInfo(existingRemoteTable)
                )

                def outline(field: BQField): BQField =
                  field.copy(
                    mode = Field.Mode.NULLABLE,
                    description = None,
                    subFields = field.subFields.map(outline)
                  )

                // materialized views are given a schema, but we cant affect it in any way.
                val patchedLocalMVDef =
                  localTableDef.copy(schema = BQSchema(localTableDef.schema.fields.map(outline)))

                if (patchedLocalMVDef == remoteAsTableDef)
                  UpdateOperation.Noop(TableDefOperationMeta(existingRemoteTable, localTableDef))
                else
                  UpdateOperation.RecreateView(
                    existingRemoteTable,
                    localTableDef,
                    createNew(localTableDef)
                  )

              case Right(wrongPartitionType) =>
                val reason =
                  s"Cannot change partitioning from $wrongPartitionType to ${localTableDef.partitionType}"
                UpdateOperation.UnsupportedPartitioning(
                  TableDefOperationMeta(existingRemoteTable, localTableDef),
                  reason
                )
              case Left(unsupported) =>
                UpdateOperation.UnsupportedPartitioning(
                  TableDefOperationMeta(existingRemoteTable, localTableDef),
                  unsupported
                )
            }

          case (
                localTableDef: BQTableDef.MaterializedView[Any],
                _: ViewDefinition
              ) =>
            UpdateOperation.RecreateView(
              existingRemoteTable,
              localTableDef,
              createNew(localTableDef)
            )

          case (localTableDef, otherDef) =>
            val reason =
              s"cannot update a ${otherDef.getType.name()} to ${localTableDef.getClass.getSimpleName}"
            UpdateOperation.Illegal(TableDefOperationMeta(existingRemoteTable, localTableDef), reason)
        }
    }

  def createNew(localTableDef: BQTableDef[Any]): UpdateOperation.CreateTable =
    localTableDef match {
      // Views must first be created without schema, then updated with schema. (2021-01-13)
      case BQTableDef.View(tableId, _, query, schema, description, labels) =>
        val withoutSchema: TableInfo =
          newTable(
            tableId.underlying,
            ViewDefinition.of(query.asStringWithUDFs),
            description,
            labels
          )

        val withSchema: TableInfo =
          withoutSchema.toBuilder
            .setDefinition(
              withoutSchema
                .getDefinition[ViewDefinition]
                .toBuilder
                .setSchema(schema.toSchema)
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
            labels
          ) =>
        val toCreate: TableInfo =
          newTable(
            tableId.underlying,
            StandardTableDefinition.newBuilder
              .setSchema(schema.toSchema)
              .setTimePartitioning(partitionType.timePartitioning.orNull)
              .setRangePartitioning(partitionType.rangePartitioning.orNull)
              .setClustering(clusteringFrom(clustering).orNull)
              .build,
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
            labels
          ) =>
        val toCreate: TableInfo =
          newTable(
            tableId.underlying,
            MaterializedViewDefinition
              .newBuilder(query.asStringWithUDFs)
              .setEnableRefresh(enableRefresh)
              // .setSchema(schema.toSchema)  // not possible for now
              .setRefreshIntervalMs(refreshIntervalMs)
              .setTimePartitioning(partitionType.timePartitioning.orNull)
              .setRangePartitioning(partitionType.rangePartitioning.orNull)
              .build(),
            description,
            labels
          )

        UpdateOperation.CreateTable(localTableDef, toCreate, None)
    }

  private def newTable(
      tableId: TableId,
      definition: TableDefinition,
      description: Option[String],
      labels: TableLabels
  ): TableInfo =
    TableInfo
      .newBuilder(tableId, definition)
      .setDescription(description.orNull)
      .setLabels(labels.forUpdate(maybeExistingTable = None))
      .build()

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