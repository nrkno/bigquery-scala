/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

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
  def withoutLocation: BQDataset = copy(location = None)
  def withId(id: String): BQDataset = copy(id = id)
  def withProject(project: ProjectId): BQDataset = copy(project = project)

  def toRef: BQDataset.Ref = BQDataset.Ref(project, id)
}

object BQDataset {
  private val regex: Pattern = "^[a-zA-Z0-9_]{1,1024}".r.pattern

  def unsafeOf(project: ProjectId, dataset: String, location: Option[LocationId] = None) =
    of(project, dataset, location).fold(err => throw new IllegalArgumentException(err), identity)

  def of(project: ProjectId, dataset: String, location: Option[LocationId] = None): Either[String, BQDataset] =
    Ref.of(project, dataset).map(ref => BQDataset(ref.project, ref.id, location))

  final case class Ref private[bigquery] (project: ProjectId, id: String)
  object Ref {
    def unsafeOf(project: ProjectId, dataset: String): Ref =
      of(project, dataset).fold(err => throw new IllegalArgumentException(err), identity)

    def of(project: ProjectId, dataset: String): Either[String, Ref] =
      if (regex.matcher(dataset).matches()) Right(Ref(project, dataset))
      else Left(s"invalid project ID '$dataset' - must match ${regex.pattern()}")
  }
}
