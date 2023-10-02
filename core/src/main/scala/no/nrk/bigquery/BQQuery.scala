/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

case class BQQuery[T: BQRead](sql: BQSqlFrag) {
  def bqRead: BQRead[T] = implicitly
}
