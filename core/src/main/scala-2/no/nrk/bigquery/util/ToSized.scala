/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.util

object ToSized {
  def apply[A](list: List[A]): Sized[IndexedSeq[A], NatUnknown] =
    Sized.wrap(list.toIndexedSeq)
}
