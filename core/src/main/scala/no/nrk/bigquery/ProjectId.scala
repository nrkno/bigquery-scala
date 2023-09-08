/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package no.nrk.bigquery

import java.util.regex.Pattern

/* From https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin:
 *
 * The project ID must be a unique string of 6 to 30 lowercase letters, digits, or hyphens. It must start with a
 * letter, and cannot have a trailing hyphen.
 */
final case class ProjectId private[bigquery] (value: String) extends AnyVal

object ProjectId {
  private val regex: Pattern = "^[a-z][a-z0-9-]{5,29}(?<!-)".r.pattern
  def fromString(input: String): Either[String, ProjectId] =
    if (regex.matcher(input).matches()) Right(new ProjectId(input))
    else Left(s"invalid project ID '$input' - must match ${regex.pattern()}")

  def unsafeFromString(input: String): ProjectId =
    fromString(input).fold(err => throw new IllegalArgumentException(err), identity)
}
