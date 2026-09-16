/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

sealed abstract class WriteDisposition(val name: String) extends Product with Serializable

object WriteDisposition {

  /** Configures the job to overwrite the table data if table already exists.
    *
    * Note that this also replaces the schema of the destination table with the schema of the written data, discarding
    * metadata which the latter cannot carry - most notably per-field descriptions. Use [[WRITE_TRUNCATE_DATA]] to
    * overwrite the rows while keeping the existing schema.
    */
  case object WRITE_TRUNCATE extends WriteDisposition("WRITE_TRUNCATE")

  /** Configures the job to overwrite the table data if table already exists, keeping the existing schema.
    *
    * Unlike [[WRITE_TRUNCATE]] this preserves table metadata such as per-field descriptions. In return the written data
    * must conform to the existing schema: the job fails with `Invalid schema update` rather than reshaping the table,
    * so any schema change has to be applied to the table before writing.
    */
  case object WRITE_TRUNCATE_DATA extends WriteDisposition("WRITE_TRUNCATE_DATA")

  /** Configures the job to append data to the table if it already exists. */
  case object WRITE_APPEND extends WriteDisposition("WRITE_APPEND")

  /** Configures the job to fail with a duplicate error if the table already exists. */
  case object WRITE_EMPTY extends WriteDisposition("WRITE_EMPTY")

  val values = List(WRITE_TRUNCATE, WRITE_TRUNCATE_DATA, WRITE_APPEND, WRITE_EMPTY)

  def fromString(name: String): WriteDisposition =
    values.find(_.name == name).getOrElse(throw new NoSuchElementException(s"$name not found"))

}
