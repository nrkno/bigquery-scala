/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.util

class IndexSeqSizedBuilder[T] {
  private val sizeBuilder = new SizedBuilder[IndexedSeq]()

  val empty: Sized[IndexedSeq[T], nat._0] = Sized.wrap[IndexedSeq[T], nat._0](IndexedSeq.empty[T])
  def apply(a: T): Sized[IndexedSeq[T], nat._1] = sizeBuilder.apply(a)

  def apply(a: T, b: T): Sized[IndexedSeq[T], nat._2] = sizeBuilder.apply(a, b)

  def apply(a: T, b: T, c: T): Sized[IndexedSeq[T], nat._3] = sizeBuilder.apply(a, b, c)

  def apply(a: T, b: T, c: T, d: T): Sized[IndexedSeq[T], nat._4] = sizeBuilder.apply(a, b, c, d)

  def apply(a: T, b: T, c: T, d: T, e: T): Sized[IndexedSeq[T], nat._5] = sizeBuilder.apply(a, b, c, d, e)

  def apply(a: T, b: T, c: T, d: T, e: T, f: T): Sized[IndexedSeq[T], nat._6] = sizeBuilder.apply(a, b, c, d, e, f)

  def apply(a: T, b: T, c: T, d: T, e: T, f: T, g: T): Sized[IndexedSeq[T], nat._7] =
    sizeBuilder.apply(a, b, c, d, e, f, g)

  def apply(a: T, b: T, c: T, d: T, e: T, f: T, g: T, h: T): Sized[IndexedSeq[T], nat._8] =
    sizeBuilder.apply(a, b, c, d, e, f, g, h)

  def apply(a: T, b: T, c: T, d: T, e: T, f: T, g: T, h: T, i: T): Sized[IndexedSeq[T], nat._9] =
    sizeBuilder.apply(a, b, c, d, e, f, g, h, i)

  def apply(a: T, b: T, c: T, d: T, e: T, f: T, g: T, h: T, i: T, j: T): Sized[IndexedSeq[T], nat._10] =
    sizeBuilder.apply(a, b, c, d, e, f, g, h, i, j)

  def apply(a: T, b: T, c: T, d: T, e: T, f: T, g: T, h: T, i: T, j: T, k: T): Sized[IndexedSeq[T], nat._11] =
    sizeBuilder.apply(a, b, c, d, e, f, g, h, i, j, k)

  def apply(a: T, b: T, c: T, d: T, e: T, f: T, g: T, h: T, i: T, j: T, k: T, l: T): Sized[IndexedSeq[T], nat._12] =
    sizeBuilder.apply(a, b, c, d, e, f, g, h, i, j, k, l)

  def apply(a: T, b: T, c: T, d: T, e: T, f: T, g: T, h: T, i: T, j: T, k: T, l: T, m: T)
      : Sized[IndexedSeq[T], nat._13] = sizeBuilder.apply(a, b, c, d, e, f, g, h, i, j, k, l, m)

  def apply(a: T, b: T, c: T, d: T, e: T, f: T, g: T, h: T, i: T, j: T, k: T, l: T, m: T, n: T)
      : Sized[IndexedSeq[T], nat._14] = sizeBuilder.apply(a, b, c, d, e, f, g, h, i, j, k, l, m, n)

  def apply(a: T, b: T, c: T, d: T, e: T, f: T, g: T, h: T, i: T, j: T, k: T, l: T, m: T, n: T, o: T)
      : Sized[IndexedSeq[T], nat._15] = sizeBuilder.apply(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)

  def apply(a: T, b: T, c: T, d: T, e: T, f: T, g: T, h: T, i: T, j: T, k: T, l: T, m: T, n: T, o: T, p: T)
      : Sized[IndexedSeq[T], nat._16] = sizeBuilder.apply(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)

  def apply(a: T, b: T, c: T, d: T, e: T, f: T, g: T, h: T, i: T, j: T, k: T, l: T, m: T, n: T, o: T, p: T, q: T)
      : Sized[IndexedSeq[T], nat._17] = sizeBuilder.apply(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)

  def apply(a: T, b: T, c: T, d: T, e: T, f: T, g: T, h: T, i: T, j: T, k: T, l: T, m: T, n: T, o: T, p: T, q: T, r: T)
      : Sized[IndexedSeq[T], nat._18] = sizeBuilder.apply(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)

  def apply(
      a: T,
      b: T,
      c: T,
      d: T,
      e: T,
      f: T,
      g: T,
      h: T,
      i: T,
      j: T,
      k: T,
      l: T,
      m: T,
      n: T,
      o: T,
      p: T,
      q: T,
      r: T,
      s: T): Sized[IndexedSeq[T], nat._19] = sizeBuilder.apply(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)

  def apply(
      a: T,
      b: T,
      c: T,
      d: T,
      e: T,
      f: T,
      g: T,
      h: T,
      i: T,
      j: T,
      k: T,
      l: T,
      m: T,
      n: T,
      o: T,
      p: T,
      q: T,
      r: T,
      s: T,
      t: T): Sized[IndexedSeq[T], nat._20] =
    sizeBuilder.apply(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)

  def apply(
      a: T,
      b: T,
      c: T,
      d: T,
      e: T,
      f: T,
      g: T,
      h: T,
      i: T,
      j: T,
      k: T,
      l: T,
      m: T,
      n: T,
      o: T,
      p: T,
      q: T,
      r: T,
      s: T,
      t: T,
      u: T): Sized[IndexedSeq[T], nat._21] =
    sizeBuilder.apply(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)

  def apply(
      a: T,
      b: T,
      c: T,
      d: T,
      e: T,
      f: T,
      g: T,
      h: T,
      i: T,
      j: T,
      k: T,
      l: T,
      m: T,
      n: T,
      o: T,
      p: T,
      q: T,
      r: T,
      s: T,
      t: T,
      u: T,
      v: T): Sized[IndexedSeq[T], nat._22] =
    sizeBuilder.apply(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
}
