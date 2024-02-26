/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.util

final class Sized[+Repr, N <: Nat](val unsized: Repr) {
  override def toString = unsized.toString

  override def equals(other: Any): Boolean =
    other match {
      case o: Sized[?, ?] => unsized == o.unsized
      case _ => false
    }

  override def hashCode: Int = unsized.hashCode
}

object Sized {

  def apply[CC[_]] = new SizedBuilder[CC]

  def wrap[Repr, L <: Nat](r: Repr): Sized[Repr, L] =
    new Sized[Repr, L](r)

  extension [A, N <: Nat](sized: Sized[IndexedSeq[A], N]) {
    def map[B](f: A => B): Sized[IndexedSeq[B], N] =
      new Sized[IndexedSeq[B], N](sized.unsized.map(f(_)))
    def length: Int = sized.unsized.length
  }
}
