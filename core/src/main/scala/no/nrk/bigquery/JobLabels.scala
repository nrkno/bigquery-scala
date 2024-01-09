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

  def validated(values: SortedMap[String, String]): Either[NonEmptyChain[String], JobLabels] =
    values.toList
      .traverse { case (key, value) =>
        (Key.apply(key), Value.apply(value)).tupled
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

  def from(params: scala.collection.immutable.Seq[(Key, Value)]): JobLabels = JobLabels(SortedMap(params.map {
    case (k, v) =>
      k.value -> v.value
  }: _*))

  def apply(params: (Key, Value)*): JobLabels = from(params.toList)

  def unsafeFrom(params: (String, String)*): JobLabels =
    validated(SortedMap(params: _*))
      .fold(messages => throw new IllegalArgumentException(messages.toList.mkString("\n")), identity)

  final class Key private (val value: String) extends AnyVal {
    override def toString: String = value
  }
  object Key {
    def apply(key: String) =
      (
        check(key)(_.isEmpty, show"label key $key is empty"),
        checkLength(key, "key"),
        checkChars(key, "key"),
        check(key)(!_(0).isLower, show"label key $key must start with lowercase letter or international character"))
        .mapN((_, _, _, _) => new Key(key))

    def unsafeFromString(value: String): Key =
      apply(value).fold(messages => throw new IllegalArgumentException(messages.toList.mkString("\n")), identity)
  }

  final class Value private (val value: String) extends AnyVal {
    override def toString: String = value
  }

  object Value {
    def apply(value: String) =
      (checkLength(value, "value"), checkChars(value, "value")).mapN((_, _) => new Value(value))

    def unsafeFromString(value: String): Value =
      apply(value).fold(messages => throw new IllegalArgumentException(messages.toList.mkString("\n")), identity)
  }

  private def check(key: String)(op: String => Boolean, msg: String) =
    if (op(key)) msg.invalidNec else key.validNec

  private def checkLength(v: String, mode: String) =
    check(v)(_.length > 63, show"label $mode $v is longer than 63 chars")

  private def checkChars(v: String, mode: String) = check(v)(
    !_.forall(c => c.isLower || c.isDigit || c == '-' || c == '_'),
    show"label $mode $v can contain only lowercase letters, numeric characters, underscores, and dashes"
  )
}
