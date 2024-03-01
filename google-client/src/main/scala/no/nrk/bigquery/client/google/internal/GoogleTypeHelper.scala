/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package client.google.internal

import com.google.cloud.bigquery.{DatasetId, StandardTableDefinition, TableDefinition, TableId, TableInfo}
import no.nrk.bigquery.TableLabels.Empty

import java.util.concurrent.TimeUnit
import scala.collection.immutable.SortedMap
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

object GoogleTypeHelper {

  def toDatasetGoogle(ds: BQDataset.Ref): DatasetId = DatasetId.of(ds.project.value, ds.id)
  def toTableIdGoogle(tableId: BQTableId): TableId =
    TableId.of(tableId.dataset.project.value, tableId.dataset.id, tableId.tableName)

  def toTableOptions(tableInfo: TableInfo): TableOptions = TableOptions(
    partitionFilterRequired = Option(tableInfo.getRequirePartitionFilter).exists(_.booleanValue()),
    tableInfo.getDefinition[TableDefinition] match {
      case definition: StandardTableDefinition =>
        Option(definition.getTimePartitioning)
          .flatMap(tp => Option(tp.getExpirationMs))
          .map(expires => FiniteDuration(expires, TimeUnit.MILLISECONDS))
      case _ => None
    }
  )

  def unsafeTableIdFromGoogle(dataset: BQDataset.Ref, tableId: TableId): BQTableId = {
    require(
      tableId.getProject == dataset.project.value && dataset.id == tableId.getDataset,
      s"Expected google table Id($tableId) to be the same datasetId and project as provided dataset[$dataset]"
    )
    BQTableId(dataset, tableId.getTable)
  }

  implicit class BQTableIdOps(val tableId: BQTableId) extends AnyVal {
    def underlying: TableId = toTableIdGoogle(tableId)
  }

  implicit class BQDatasetOps(val ds: BQDataset) extends AnyVal {
    def underlying: DatasetId = toDatasetGoogle(ds.toRef)
  }

  implicit class BQDatasetRefOps(val ds: BQDataset.Ref) extends AnyVal {
    def underlying: DatasetId = toDatasetGoogle(ds)
  }

  def tableLabelsfromTableInfo(tableInfo: TableInfo): TableLabels =
    Option(tableInfo.getLabels) match {
      case Some(values) => new TableLabels(SortedMap(values.asScala.toList*))
      case None => Empty
    }
}
