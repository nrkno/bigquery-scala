/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.data.NonEmptyChain

import scala.annotation.implicitNotFound
import scala.collection.immutable.SortedMap

// https://cloud.google.com/bigquery/docs/labels-intro#requirements
@implicitNotFound("No joblabels instance was found, make sure you have an implicit or given instance")
final case class JobLabels private (value: SortedMap[String, String]) {
  def ++(other: JobLabels): JobLabels =
    JobLabels(other.value ++ this.value)
}

object JobLabels {
  import cats.syntax.all.*

  val empty = JobLabels(SortedMap.empty[String, String])

  def validated(values: SortedMap[String, String]): Either[NonEmptyChain[String], JobLabels] =
    values.toList
      .traverse { case (key, value) =>
        (Labels.Key.apply(key), Labels.Value.apply(value)).tupled
      }
      .map(list => JobLabels.from(list))
      .toEither

  /** truncates string to 63 chars. Replaces all non-word chars with _ Users should make sure this cannot result in an
    * empty string, as that will fail validation,
    *
    * @param value
    *   a non-empty string
    * @return
    *   a truncated or empty string.
    */
  def safeString(value: String): String =
    value.trim.substring(0, Math.min(63, value.length)).toLowerCase().replaceAll("\\W", "_")

  def from(params: scala.collection.immutable.Seq[(Labels.Key, Labels.Value)]): JobLabels = JobLabels(
    SortedMap(params.map { case (k, v) =>
      k.value -> v.value
    }*))

  def apply(params: (Labels.Key, Labels.Value)*): JobLabels = from(params.toList)

  def unsafeFrom(params: (String, String)*): JobLabels =
    validated(SortedMap(params*))
      .fold(messages => throw new IllegalArgumentException(messages.toList.mkString("\n")), identity)
}
