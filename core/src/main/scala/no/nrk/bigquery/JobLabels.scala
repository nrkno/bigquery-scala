/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.data.NonEmptyChain

import scala.annotation.implicitNotFound
import scala.collection.immutable.SortedMap

@implicitNotFound("No joblabels instance was found, make sure you have an implicit or given instance")
final case class JobLabels private (value: SortedMap[String, String]) {
  def ++(other: JobLabels): JobLabels =
    JobLabels(other.value ++ this.value)
}

object JobLabels {
  import cats.syntax.all.*

  val empty = JobLabels(SortedMap.empty[String, String])

  def validated(values: SortedMap[String, String]): Either[NonEmptyChain[String], JobLabels] = {
    def check(key: String)(op: String => Boolean, msg: String) =
      if (op(key)) msg.invalidNec else key.validNec

    def checkLength(v: String, mode: String) = check(v)(_.length > 63, show"label $mode $v is longer than 63 chars")
    def checkChars(v: String, mode: String) = check(v)(
      !_.forall(c => c.isLower || c.isDigit || c == '-' || c == '_'),
      show"label $mode $v can contain only lowercase letters, numeric characters, underscores, and dashes"
    )

    values.toList
      .traverse { case (key, value) =>
        val checkKey =
          (
            check(key)(_.isEmpty, show"label key $key is empty"),
            checkLength(key, "key"),
            checkChars(key, "key"),
            check(key)(_(0).isLower, show"label key $key must start with lowercase letter or international character"))
            .mapN((_, _, _, _) => key)

        val checkValue = (checkLength(value, "value"), checkChars(value, "value")).mapN((_, _) => value)

        (checkKey, checkValue).tupled
      }
      .map(list => JobLabels(SortedMap(list: _*)))
      .toEither
  }

  def safeString(value: String): Option[String] =
    if (value.trim.isEmpty) None
    else Some(value.substring(0, Math.min(63, value.length)).toLowerCase().replaceAll("\\W", "-"))

  def apply(params: (String, String)*): Either[NonEmptyChain[String], JobLabels] = validated(SortedMap(params: _*))

  def unsafeFrom(params: (String, String)*): JobLabels =
    validated(SortedMap(params: _*))
      .fold(messages => throw new IllegalArgumentException(messages.toList.mkString("\n")), identity)

}
