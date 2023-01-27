package no.nrk.bigquery

import cats.Show
import com.google.cloud.bigquery.TableId
import io.circe.{Codec, Decoder, Encoder}

final case class LocationId(value: String) extends AnyVal

object LocationId {
  implicit val codec: Codec[LocationId] = Codec.from(
    Decoder.decodeString.map(LocationId(_)),
    Encoder.encodeString.contramap(_.value)
  )
}

final case class ProjectId(value: String) extends AnyVal

object ProjectId {
  implicit val codec: Codec[ProjectId] = Codec.from(
    Decoder.decodeString.map(ProjectId(_)),
    Encoder.encodeString.contramap(_.value)
  )
}

final case class BQDataset(
    project: ProjectId,
    id: String,
    location: Option[LocationId]
)

object BQDataset {
  def of(project: ProjectId, dataset: String) =
    BQDataset(project, dataset, None)

  implicit val encoder: Encoder[BQDataset] =
    Encoder.forProduct3("project", "dataset", "location")(dataset =>
      (dataset.project, dataset.id, dataset.location)
    )
  implicit val decoder: Decoder[BQDataset] =
    Decoder.forProduct3(
      "project",
      "dataset",
      "table"
    )(apply)
}
final case class BQTableId(dataset: BQDataset, tableName: String) {

  def modifyTableName(f: String => String): BQTableId =
    copy(tableName = f(tableName))
  def underlying: TableId =
    TableId.of(dataset.project.value, dataset.id, tableName)

  def asString: String = s"${dataset.project.value}.${dataset.id}.${tableName}"
  def asFragment: BQSqlFrag = BQSqlFrag(asString)
}

object BQTableId {
  def of(project: ProjectId, dataset: String, tableName: String) =
    BQTableId(BQDataset.of(project, dataset), tableName)

  def unsafeFromGoogle(dataset: BQDataset, tableId: TableId) = {
    require(
      tableId.getProject == dataset.project.value && dataset.id == tableId.getDataset,
      s"Expected google table Id to be the same datasetId and project as provided dataset[$dataset]"
    )
    BQTableId(dataset, tableId.getTable)
  }

  implicit val show: Show[BQTableId] =
    Show.show(_.asString)

  implicit val tableIdEncoder: Encoder[BQTableId] =
    Encoder.forProduct2("dataset", "table")(table =>
      (table.dataset, table.tableName)
    )
  implicit val tableIdDecoder: Decoder[BQTableId] =
    Decoder.forProduct2(
      "dataset",
      "table"
    )(apply)

  // TODO: Maybe implement BQShow
}
