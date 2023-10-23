/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.Show

final case class BQReference private[bigquery] (projectId: ProjectId, dataset: String, resource: String) {

  def modifyResource(f: String => String): BQReference =
    withResource(f(resource))

  def withDataset(ds: String): BQReference = BQReference.unsafeFromString(copy(dataset = ds).asString)
  def withProject(project: ProjectId): BQReference = BQReference.unsafeFromString(copy(projectId = project).asString)
  def withResource(r: String): BQReference = BQReference.unsafeFromString(copy(resource = r).asString)

  def asString: String = s"${projectId.value}.${dataset}.${resource}"
  def asFragment: BQSqlFrag = BQSqlFrag.backticks(asString)
}

object BQReference {
  def fromTableId(tableId: BQTableId): BQReference =
    BQReference(tableId.dataset.project, tableId.dataset.id, tableId.tableName)

  def fromUdf(udf: UDF.UDFId.PersistentId): BQReference =
    BQReference(udf.dataset.project, udf.dataset.id, udf.name.value)

  def unsafeFrom(project: ProjectId, dataset: String, tableName: String): BQReference =
    unsafeFromString(s"${project.value}.${dataset}.${tableName}")

  def unsafeFromString(id: String): BQReference =
    fromString(id).fold(
      err => throw new IllegalArgumentException(err),
      identity
    )

  def fromString(id: String): Either[String, BQReference] =
    BQTableId.fromString(id).map(fromTableId)

  implicit val show: Show[BQReference] =
    Show.show(_.asFragment.asString)

  implicit val ordering: Ordering[BQReference] = Ordering.by(_.asString)

  // TODO: Maybe implement BQShow
}
