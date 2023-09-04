/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package no.nrk.bigquery

import cats.Show

import java.util.regex.Pattern

/** When you create a table in BigQuery, the table name must be unique per dataset. The table name can:
  *
  *   - Contain up to 1,024 characters.
  *   - Contain Unicode characters in category L (letter), M (mark), N (number), Pc (connector, including underscore),
  *     Pd (dash), Zs (space).
  *
  * FROM https://cloud.google.com/bigquery/docs/tables#table_naming
  */
final case class BQTableId private[bigquery] (dataset: BQDataset, tableName: String) {

  def modifyTableName(f: String => String): BQTableId =
    BQTableId.unsafeOf(dataset, f(tableName))

  def withLocation(locationId: Option[LocationId]) = withDataset(dataset.copy(location = locationId))
  def withDataset(ds: BQDataset) = copy(dataset = ds)

  def asString: String = s"${dataset.project.value}.${dataset.id}.${tableName}"
  def asFragment: BQSqlFrag = BQSqlFrag.backticks(asString)
}

object BQTableId {

  private val regex: Pattern = "(?U)^\\w[\\w_ *$-]{1,1023}".r.pattern

  def of(dataset: BQDataset, tableName: String): Either[String, BQTableId] =
    if (regex.matcher(tableName).matches()) Right(BQTableId(dataset, tableName))
    else Left(s"Expected '$tableName' to match regex (${regex.pattern()})")

  def unsafeOf(dataset: BQDataset, tableName: String): BQTableId =
    of(dataset, tableName).fold(err => throw new IllegalArgumentException(err), identity)

  def unsafeFrom(project: ProjectId, dataset: String, tableName: String): BQTableId =
    unsafeFromString(s"${project.value}.${dataset}.${tableName}")

  def unsafeFromString(id: String): BQTableId =
    fromString(id).fold(
      err => throw new IllegalArgumentException(err),
      identity
    )

  def fromString(id: String): Either[String, BQTableId] =
    id.split("\\.", 3) match {
      case Array(project, dataset, tableName) =>
        ProjectId.fromString(project).flatMap(BQDataset.of(_, dataset)).flatMap(of(_, tableName))
      case _ => Left(s"Expected [projectId].[datasetId].[tableName] but got ${id}")
    }

  implicit val show: Show[BQTableId] =
    Show.show(_.asFragment.asString)

  implicit val orderingTableId: Ordering[BQTableId] = Ordering.by(_.asString)

  // TODO: Maybe implement BQShow
}
