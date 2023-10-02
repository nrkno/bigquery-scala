/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

sealed trait BQLimit

object BQLimit {
  case class Limit(value: Int) extends BQLimit
  case object NoLimit extends BQLimit
}
