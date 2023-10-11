/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.Show

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
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
  def asPathString: String = s"projects/${dataset.project.value}/datasets/${dataset.id}/tables/${tableName}"
  def asFragment: BQSqlFrag = BQSqlFrag.backticks(asString)
}

object BQTableId {

  private val regex: Pattern = "(?U)^\\w[\\w_ *$-]{1,1023}".r.pattern
  private val PathPattern = "/?projects/(.*)/datasets/(.*)/tables/(.*)".r
  private lazy val example = BQTableId(BQDataset(ProjectId("projectId"), "datasetId", None), "tableName")

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

  def fromString(id: String): Either[String, BQTableId] = {
    def decode(s: String) = URLDecoder.decode(s, StandardCharsets.UTF_8.name())

    val fromPath = id match {
      case PathPattern(project, dataset, tableName) =>
        s"${decode(project)}.${decode(dataset)}.${decode(tableName)}"
      case x => x
    }

    fromPath.split("\\.", 3) match {
      case Array(project, dataset, tableName) =>
        ProjectId.fromString(project).flatMap(BQDataset.of(_, dataset)).flatMap(of(_, tableName))
      case _ => Left(s"Expected id formatted as'${example.asString}' or '${example.asPathString}' but got ${id}")
    }
  }

  implicit val show: Show[BQTableId] =
    Show.show(_.asFragment.asString)

  implicit val orderingTableId: Ordering[BQTableId] = Ordering.by(_.asString)

  // TODO: Maybe implement BQShow
}
