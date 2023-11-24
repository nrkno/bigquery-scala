package no.nrk.bigquery

import io.circe.{Encoder, Json}
import io.circe.syntax.*

trait HashedEncoder[A, T] {
  def toJson(a: A, partitionType: BQPartitionType.RangePartitioned): Json
}

object HashedEncoder {
  def instance[A, T](get: A => T)(implicit e: Encoder[A], h: BQValueHasher[T]): HashedEncoder[A, T] =
    (a: A, partitionType: BQPartitionType.RangePartitioned) => {
      val json = e.apply(a)
      json.deepMerge(Json.obj(partitionType.field.value := h.hashValue(get(a), partitionType.range)))
    }
}
