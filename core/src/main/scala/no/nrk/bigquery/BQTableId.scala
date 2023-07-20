package no.nrk.bigquery

import cats.Show
import com.google.cloud.bigquery.{DatasetId, TableId}

import java.util.regex.Pattern

final case class LocationId(value: String) extends AnyVal

object LocationId {
  val US = LocationId("us")
  val EU = LocationId("eu")

  val USWest2 = LocationId("us-west2")
  val LosAngeles = USWest2

  val NorthAmericaNorthEast1 = LocationId("northamerica-northeast1")
  val Montreal = NorthAmericaNorthEast1

  val UsEast4 = LocationId("us-east4")
  val NorthernVirginia = UsEast4

  val SouthAmericaEast1 = LocationId("southamerica-east1")
  val SaoPaulo = SouthAmericaEast1

  val EuropeNorth1 = LocationId("europe-north1")
  val Finland = EuropeNorth1

  val EuropeWest2 = LocationId("europe-west2")
  val London = EuropeWest2

  val EuropeWest6 = LocationId("europe-west6")
  val Zurich = EuropeWest6

  val AsiaEast2 = LocationId("asia-east2")
  val HongKong = AsiaEast2

  val AsiaSouth1 = LocationId("asia-south1")
  val Mumbai = AsiaSouth1

  val AsiaNorthEast2 = LocationId("asia-northeast2")
  val Osaka = AsiaNorthEast2

  val AsiaEast1 = LocationId("asia-east1")
  val Taiwan = AsiaEast1

  val AsiaNorthEast1 = LocationId("asia-northeast1")
  val Tokyo = AsiaNorthEast1

  val AsiaSouthEast1 = LocationId("asia-southeast1")
  val Singapore = AsiaSouthEast1

  val AustraliaSouthEast1 = LocationId("australia-southeast1")
  val Sydney = AustraliaSouthEast1
}

final case class ProjectId(value: String) extends AnyVal

object ProjectId {
  /* From https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin:
   *
   * The project ID must be a unique string of 6 to 30 lowercase letters, digits, or hyphens. It must start with a
   * letter, and cannot have a trailing hyphen.
   */
  private val regex: Pattern = "^[a-z][a-z0-9-]{5,29}(?<!-)".r.pattern
  def fromString(input: String): Either[String, ProjectId] =
    if (regex.matcher(input).matches()) Right(new ProjectId(input))
    else Left(s"invalid project ID '$input' - must match ${regex.pattern()}")

  def unsafeFromString(input: String): ProjectId =
    fromString(input).fold(err => throw new IllegalArgumentException(err), identity)
}

final case class BQDataset(
    project: ProjectId,
    id: String,
    location: Option[LocationId]
) {
  def underlying: DatasetId = DatasetId.of(project.value, id)

  def withLocation(locationId: LocationId): BQDataset = copy(location = Some(locationId))
}

object BQDataset {

  /** FROM https://cloud.google.com/bigquery/docs/datasets#dataset-naming
    */
  private val regex: Pattern = "^[a-zA-Z0-9_]{1,1024}".r.pattern

  def of(project: ProjectId, dataset: String) =
    fromId(project, dataset).fold(err => throw new IllegalArgumentException(err), identity)

  def fromId(project: ProjectId, dataset: String): Either[String, BQDataset] =
    if (regex.matcher(dataset).matches()) Right(BQDataset(project, dataset, None))
    else Left(s"invalid project ID '$dataset' - must match ${regex.pattern()}")

}
final case class BQTableId(dataset: BQDataset, tableName: String) {

  def modifyTableName(f: String => String): BQTableId =
    BQTableId.unsafeOfTable(dataset, f(tableName))
  def underlying: TableId =
    TableId.of(dataset.project.value, dataset.id, tableName)

  def withLocation(locationId: Option[LocationId]) = withDataset(dataset.copy(location = locationId))
  def withDataset(ds: BQDataset) = copy(dataset = ds)

  def asString: String = s"${dataset.project.value}.${dataset.id}.${tableName}"
  def asFragment: BQSqlFrag = BQSqlFrag.backticks(asString)
}

object BQTableId {

  /** FROM https://cloud.google.com/bigquery/docs/tables#table_naming
    */
  private val regex: Pattern = "^[\\w_ -]{1,1024}".r.pattern

  def of(project: ProjectId, dataset: String, tableName: String) =
    unsafeOfTable(BQDataset.of(project, dataset), tableName)

  def ofTable(dataset: BQDataset, tableName: String): Either[String, BQTableId] =
    if (regex.matcher(tableName).matches()) Right(BQTableId(dataset, tableName))
    else Left(s"Expected $tableName to match regex (${regex.pattern()})")
  def unsafeOfTable(dataset: BQDataset, tableName: String): BQTableId =
    ofTable(dataset, tableName).fold(err => throw new IllegalArgumentException(err), identity)

  def unsafeFromGoogle(dataset: BQDataset, tableId: TableId): BQTableId = {
    require(
      tableId.getProject == dataset.project.value && dataset.id == tableId.getDataset,
      s"Expected google table Id($tableId) to be the same datasetId and project as provided dataset[$dataset]"
    )
    BQTableId(dataset, tableId.getTable)
  }

  def unsafeFromString(id: String): BQTableId =
    fromString(id).fold(
      err => throw new IllegalArgumentException(err),
      identity
    )

  def fromString(id: String): Either[String, BQTableId] =
    id.split("\\.", 3) match {
      case Array(project, dataset, tableName) =>
        ProjectId.fromString(project).flatMap(BQDataset.fromId(_, dataset)).flatMap(ofTable(_, tableName))
      case _ => Left(s"Expected [projectId].[datasetId].[tableName] but got ${id}")
    }

  implicit val show: Show[BQTableId] =
    Show.show(_.asFragment.asString)

  implicit val orderingTableId: Ordering[BQTableId] = Ordering.by(_.asString)

  // TODO: Maybe implement BQShow
}
