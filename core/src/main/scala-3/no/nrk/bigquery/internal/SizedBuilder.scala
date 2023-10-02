/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.internal

class SizedBuilder[CC[_]] {
  def apply[A](a1: A) = new Sized[IndexedSeq[A], nat._1](IndexedSeq(a1))
  def apply[A](a1: A, a2: A) = new Sized[IndexedSeq[A], nat._2](IndexedSeq(a1, a2))
  def apply[A](a1: A, a2: A, a3: A) = new Sized[IndexedSeq[A], nat._3](IndexedSeq(a1, a2, a3))
  def apply[A](a1: A, a2: A, a3: A, a4: A) = new Sized[IndexedSeq[A], nat._4](IndexedSeq(a1, a2, a3, a4))
  def apply[A](a1: A, a2: A, a3: A, a4: A, a5: A) = new Sized[IndexedSeq[A], nat._5](IndexedSeq(a1, a2, a3, a4, a5))
  def apply[A](a1: A, a2: A, a3: A, a4: A, a5: A, a6: A) =
    new Sized[IndexedSeq[A], nat._6](IndexedSeq(a1, a2, a3, a4, a5, a6))
  def apply[A](a1: A, a2: A, a3: A, a4: A, a5: A, a6: A, a7: A) =
    new Sized[IndexedSeq[A], nat._7](IndexedSeq(a1, a2, a3, a4, a5, a6, a7))
  def apply[A](a1: A, a2: A, a3: A, a4: A, a5: A, a6: A, a7: A, a8: A) =
    new Sized[IndexedSeq[A], nat._8](IndexedSeq(a1, a2, a3, a4, a5, a6, a7, a8))
  def apply[A](a1: A, a2: A, a3: A, a4: A, a5: A, a6: A, a7: A, a8: A, a9: A) =
    new Sized[IndexedSeq[A], nat._9](IndexedSeq(a1, a2, a3, a4, a5, a6, a7, a8, a9))
  def apply[A](a1: A, a2: A, a3: A, a4: A, a5: A, a6: A, a7: A, a8: A, a9: A, a10: A) =
    new Sized[IndexedSeq[A], nat._10](IndexedSeq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10))
  def apply[A](a1: A, a2: A, a3: A, a4: A, a5: A, a6: A, a7: A, a8: A, a9: A, a10: A, a11: A) =
    new Sized[IndexedSeq[A], nat._11](IndexedSeq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11))
  def apply[A](a1: A, a2: A, a3: A, a4: A, a5: A, a6: A, a7: A, a8: A, a9: A, a10: A, a11: A, a12: A) =
    new Sized[IndexedSeq[A], nat._12](IndexedSeq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12))
  def apply[A](a1: A, a2: A, a3: A, a4: A, a5: A, a6: A, a7: A, a8: A, a9: A, a10: A, a11: A, a12: A, a13: A) =
    new Sized[IndexedSeq[A], nat._13](IndexedSeq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13))
  def apply[A](a1: A, a2: A, a3: A, a4: A, a5: A, a6: A, a7: A, a8: A, a9: A, a10: A, a11: A, a12: A, a13: A, a14: A) =
    new Sized[IndexedSeq[A], nat._14](IndexedSeq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14))
  def apply[A](
      a1: A,
      a2: A,
      a3: A,
      a4: A,
      a5: A,
      a6: A,
      a7: A,
      a8: A,
      a9: A,
      a10: A,
      a11: A,
      a12: A,
      a13: A,
      a14: A,
      a15: A) =
    new Sized[IndexedSeq[A], nat._15](IndexedSeq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15))
  def apply[A](
      a1: A,
      a2: A,
      a3: A,
      a4: A,
      a5: A,
      a6: A,
      a7: A,
      a8: A,
      a9: A,
      a10: A,
      a11: A,
      a12: A,
      a13: A,
      a14: A,
      a15: A,
      a16: A) =
    new Sized[IndexedSeq[A], nat._16](IndexedSeq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16))
  def apply[A](
      a1: A,
      a2: A,
      a3: A,
      a4: A,
      a5: A,
      a6: A,
      a7: A,
      a8: A,
      a9: A,
      a10: A,
      a11: A,
      a12: A,
      a13: A,
      a14: A,
      a15: A,
      a16: A,
      a17: A) = new Sized[IndexedSeq[A], nat._17](
    IndexedSeq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17))
  def apply[A](
      a1: A,
      a2: A,
      a3: A,
      a4: A,
      a5: A,
      a6: A,
      a7: A,
      a8: A,
      a9: A,
      a10: A,
      a11: A,
      a12: A,
      a13: A,
      a14: A,
      a15: A,
      a16: A,
      a17: A,
      a18: A) = new Sized[IndexedSeq[A], nat._18](
    IndexedSeq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18))
  def apply[A](
      a1: A,
      a2: A,
      a3: A,
      a4: A,
      a5: A,
      a6: A,
      a7: A,
      a8: A,
      a9: A,
      a10: A,
      a11: A,
      a12: A,
      a13: A,
      a14: A,
      a15: A,
      a16: A,
      a17: A,
      a18: A,
      a19: A) = new Sized[IndexedSeq[A], nat._19](
    IndexedSeq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19))
  def apply[A](
      a1: A,
      a2: A,
      a3: A,
      a4: A,
      a5: A,
      a6: A,
      a7: A,
      a8: A,
      a9: A,
      a10: A,
      a11: A,
      a12: A,
      a13: A,
      a14: A,
      a15: A,
      a16: A,
      a17: A,
      a18: A,
      a19: A,
      a20: A) = new Sized[IndexedSeq[A], nat._20](
    IndexedSeq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20))
  def apply[A](
      a1: A,
      a2: A,
      a3: A,
      a4: A,
      a5: A,
      a6: A,
      a7: A,
      a8: A,
      a9: A,
      a10: A,
      a11: A,
      a12: A,
      a13: A,
      a14: A,
      a15: A,
      a16: A,
      a17: A,
      a18: A,
      a19: A,
      a20: A,
      a21: A) = new Sized[IndexedSeq[A], nat._21](
    IndexedSeq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21))
  def apply[A](
      a1: A,
      a2: A,
      a3: A,
      a4: A,
      a5: A,
      a6: A,
      a7: A,
      a8: A,
      a9: A,
      a10: A,
      a11: A,
      a12: A,
      a13: A,
      a14: A,
      a15: A,
      a16: A,
      a17: A,
      a18: A,
      a19: A,
      a20: A,
      a21: A,
      a22: A) = new Sized[IndexedSeq[A], nat._22](
    IndexedSeq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22))

}
