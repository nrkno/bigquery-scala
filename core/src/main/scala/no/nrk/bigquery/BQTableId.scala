package no.nrk.bigquery

import cats.Show
import com.google.cloud.bigquery.{DatasetId, TableId}

final case class LocationId(value: String) extends AnyVal

final case class ProjectId(value: String) extends AnyVal

final case class BQDataset(
    project: ProjectId,
    id: String,
    location: Option[LocationId]
) {
  def underlying: DatasetId = DatasetId.of(project.value, id)
}

object BQDataset {
  def of(project: ProjectId, dataset: String) =
    BQDataset(project, dataset, None)
}
final case class BQTableId(dataset: BQDataset, tableName: String) {

  def modifyTableName(f: String => String): BQTableId =
    copy(tableName = f(tableName))
  def underlying: TableId =
    TableId.of(dataset.project.value, dataset.id, tableName)

  def withLocation(locationId: Option[LocationId]) = withDataset(dataset.copy(location = locationId))
  def withDataset(ds: BQDataset) = copy(dataset = ds)

  def asString: String = s"${dataset.project.value}.${dataset.id}.${tableName}"
  def asFragment: BQSqlFrag = BQSqlFrag.backticks(asString)
}

object BQTableId {
  def of(project: ProjectId, dataset: String, tableName: String) =
    BQTableId(BQDataset.of(project, dataset), tableName)

  def unsafeFromGoogle(dataset: BQDataset, tableId: TableId): BQTableId = {
    require(
      tableId.getProject == dataset.project.value && dataset.id == tableId.getDataset,
      s"Expected google table Id($tableId) to be the same datasetId and project as provided dataset[$dataset]"
    )
    BQTableId(dataset, tableId.getTable)
  }

  implicit val show: Show[BQTableId] =
    Show.show(_.asFragment.asString)

  implicit val orderingTableId: Ordering[BQTableId] = Ordering.by(_.asString)

  // TODO: Maybe implement BQShow
}
