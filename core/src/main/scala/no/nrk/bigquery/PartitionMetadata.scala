package no.nrk.bigquery

import cats.Show
import io.circe.{Decoder, Encoder}

import java.time.Instant

case class PartitionMetadata(
    creationTime: Option[Instant],
    lastModifiedTime: Option[Instant],
    rowCount: Option[Long],
    sizeBytes: Option[Long]
)
object PartitionMetadata {
  implicit val decoder: Decoder[PartitionMetadata] = Decoder.forProduct4(
    "creationTime",
    "lastModifiedTime",
    "rowCount",
    "sizeBytes"
  )(apply)
  implicit val encoder: Encoder[PartitionMetadata] = Encoder.forProduct4(
    "creationTime",
    "lastModifiedTime",
    "rowCount",
    "sizeBytes"
  )(x => (x.creationTime, x.lastModifiedTime, x.rowCount, x.sizeBytes))
  implicit val shows: Show[PartitionMetadata] =
    Show.fromToString[PartitionMetadata]

  val Empty = PartitionMetadata(None, None, None, None)
}
