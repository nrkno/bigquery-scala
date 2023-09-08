/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package no.nrk.bigquery

import cats.Show

import java.time.Instant

case class PartitionMetadata(
    creationTime: Option[Instant],
    lastModifiedTime: Option[Instant],
    rowCount: Option[Long],
    sizeBytes: Option[Long]
)
object PartitionMetadata {
  implicit val shows: Show[PartitionMetadata] =
    Show.fromToString[PartitionMetadata]

  val Empty = PartitionMetadata(None, None, None, None)
}
