/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

sealed abstract class WriteDisposition(val name: String) extends Product with Serializable

object WriteDisposition {

  /** Configures the job to overwrite the table data if table already exists. */
  case object WRITE_TRUNCATE extends WriteDisposition("WRITE_TRUNCATE")

  /** Configures the job to append data to the table if it already exists. */
  case object WRITE_APPEND extends WriteDisposition("WRITE_APPEND")

  /** Configures the job to fail with a duplicate error if the table already exists. */
  case object WRITE_EMPTY extends WriteDisposition("WRITE_EMPTY")

  val values = List(WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY)

  def fromString(name: String): WriteDisposition =
    values.find(_.name == name).getOrElse(throw new NoSuchElementException(s"$name not found"))

}
