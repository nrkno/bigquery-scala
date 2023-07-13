package no.nrk.bigquery.internal

import com.google.cloud.bigquery.{DatasetId, TableId}
import no.nrk.bigquery.{BQDataset, BQTableId}

object GoogleTypeHelper {

  def toDatasetGoogle(ds: BQDataset): DatasetId = DatasetId.of(ds.project.value, ds.id)
  def toTableIdGoogle(tableId: BQTableId): TableId =
    TableId.of(tableId.dataset.project.value, tableId.dataset.id, tableId.tableName)

  def unsafeTableIdFromGoogle(dataset: BQDataset, tableId: TableId): BQTableId = {
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
    def underlying: DatasetId = toDatasetGoogle(ds)
  }
}
