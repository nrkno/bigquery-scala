/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

/** A string literal in an sql statement, it will be quoted in the final SQL
  */
case class StringValue(value: String)
