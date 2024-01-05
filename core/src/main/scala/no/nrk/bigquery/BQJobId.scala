/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.data.NonEmptyChain

import scala.collection.immutable.SortedMap

final case class BQJobId(
    projectId: Option[ProjectId],
    locationId: Option[LocationId],
    name: BQJobName,
    labels: JobLabels
) {

  def withProjectId(projectId: ProjectId) = copy(projectId = Some(projectId))
  def withLocationId(locationId: LocationId) = copy(locationId = Some(locationId))
  def withLabels(labels: JobLabels) = copy(labels = labels)

  private[bigquery] def withDefaults(defaults: Option[BQClientDefaults]) =
    if (projectId.isEmpty && locationId.isEmpty) {
      copy(projectId = defaults.map(_.projectId), locationId = defaults.map(_.locationId))
    } else this

  def +(str: String): BQJobId = copy(name = name + str)
  def append(str: String): BQJobId = copy(name = name + str)
}

object BQJobId {
  def auto(implicit enclosing: sourcecode.Enclosing): BQJobId =
    apply(enclosing.value)

  def apply(name: String): BQJobId = apply(name, JobLabels.empty)
  def apply(name: String, labels: JobLabels): BQJobId = BQJobId(None, None, BQJobName(name), labels)
}

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
      .map(list => JobLabels(SortedMap.from(list)))
      .toEither
  }

  def safeString(value: String): Option[String] =
    if (value.trim.isEmpty) None else Some(value.substring(0, Math.min(63, value.length)).replaceAll("\\W", "-"))

  def apply(params: (String, String)*): Either[NonEmptyChain[String], JobLabels] = validated(SortedMap.from(params))

  def unsafeFrom(params: (String, String)*): JobLabels =
    validated(SortedMap.from(params))
      .fold(messages => throw new IllegalArgumentException(messages.toList.mkString("\n")), identity)

}
