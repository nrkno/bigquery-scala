package no.nrk.bigquery

import java.util.regex.Pattern

/** When you create a dataset in BigQuery, the dataset name must be unique for each project. The dataset name can
  * contain the following:
  *
  *   - Up to 1,024 characters.
  *   - Letters (uppercase or lowercase), numbers, and underscores.
  *
  * FROM https://cloud.google.com/bigquery/docs/datasets#dataset-naming
  */
final case class BQDataset private[bigquery] (
    project: ProjectId,
    id: String,
    location: Option[LocationId]
) {

  def withLocation(locationId: LocationId): BQDataset = copy(location = Some(locationId))
}

object BQDataset {
  private val regex: Pattern = "^[a-zA-Z0-9_]{1,1024}".r.pattern

  def unsafeOf(project: ProjectId, dataset: String) =
    of(project, dataset).fold(err => throw new IllegalArgumentException(err), identity)

  def of(project: ProjectId, dataset: String): Either[String, BQDataset] =
    if (regex.matcher(dataset).matches()) Right(BQDataset(project, dataset, None))
    else Left(s"invalid project ID '$dataset' - must match ${regex.pattern()}")
}
