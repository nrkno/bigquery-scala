/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.internal

import no.nrk.bigquery.{BQRoutine, BQSqlFrag}
import no.nrk.bigquery.util.nat._
import no.nrk.bigquery.util.IndexSeqSizedBuilder

trait RoutineSyntax {
  implicit def bqRoutineOps0(routine: BQRoutine[_0]): RoutineOps0 = new RoutineOps0(routine)
  implicit def bqRoutineOps1(routine: BQRoutine[_1]): RoutineOps1 = new RoutineOps1(routine)
  implicit def bqRoutineOps2(routine: BQRoutine[_2]): RoutineOps2 = new RoutineOps2(routine)
  implicit def bqRoutineOps3(routine: BQRoutine[_3]): RoutineOps3 = new RoutineOps3(routine)
  implicit def bqRoutineOps4(routine: BQRoutine[_4]): RoutineOps4 = new RoutineOps4(routine)
  implicit def bqRoutineOps5(routine: BQRoutine[_5]): RoutineOps5 = new RoutineOps5(routine)
  implicit def bqRoutineOps6(routine: BQRoutine[_6]): RoutineOps6 = new RoutineOps6(routine)
  implicit def bqRoutineOps7(routine: BQRoutine[_7]): RoutineOps7 = new RoutineOps7(routine)
  implicit def bqRoutineOps8(routine: BQRoutine[_8]): RoutineOps8 = new RoutineOps8(routine)
  implicit def bqRoutineOps9(routine: BQRoutine[_9]): RoutineOps9 = new RoutineOps9(routine)
  implicit def bqRoutineOps10(routine: BQRoutine[_10]): RoutineOps10 = new RoutineOps10(routine)
  implicit def bqRoutineOps11(routine: BQRoutine[_11]): RoutineOps11 = new RoutineOps11(routine)
  implicit def bqRoutineOps12(routine: BQRoutine[_12]): RoutineOps12 = new RoutineOps12(routine)
  implicit def bqRoutineOps13(routine: BQRoutine[_13]): RoutineOps13 = new RoutineOps13(routine)
  implicit def bqRoutineOps14(routine: BQRoutine[_14]): RoutineOps14 = new RoutineOps14(routine)
  implicit def bqRoutineOps15(routine: BQRoutine[_15]): RoutineOps15 = new RoutineOps15(routine)
  implicit def bqRoutineOps16(routine: BQRoutine[_16]): RoutineOps16 = new RoutineOps16(routine)
  implicit def bqRoutineOps17(routine: BQRoutine[_17]): RoutineOps17 = new RoutineOps17(routine)
  implicit def bqRoutineOps18(routine: BQRoutine[_18]): RoutineOps18 = new RoutineOps18(routine)
  implicit def bqRoutineOps19(routine: BQRoutine[_19]): RoutineOps19 = new RoutineOps19(routine)
  implicit def bqRoutineOps20(routine: BQRoutine[_20]): RoutineOps20 = new RoutineOps20(routine)
  implicit def bqRoutineOps21(routine: BQRoutine[_21]): RoutineOps21 = new RoutineOps21(routine)
  implicit def bqRoutineOps22(routine: BQRoutine[_22]): RoutineOps22 = new RoutineOps22(routine)
}

object RoutineSyntax {
  private[internal] val builder = new IndexSeqSizedBuilder[BQSqlFrag.Magnet]
}

class RoutineOps0(routine: BQRoutine[_0]) {
  def apply(): BQSqlFrag =
    routine.call(RoutineSyntax.builder.empty)
}

class RoutineOps1(routine: BQRoutine[_1]) {
  def apply(
      m1: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(RoutineSyntax.builder.apply(m1))
}

class RoutineOps2(routine: BQRoutine[_2]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(RoutineSyntax.builder.apply(m1, m2))
}

class RoutineOps3(routine: BQRoutine[_3]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(RoutineSyntax.builder.apply(m1, m2, m3))
}

class RoutineOps4(routine: BQRoutine[_4]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4))
}

class RoutineOps5(routine: BQRoutine[_5]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5))
}

class RoutineOps6(routine: BQRoutine[_6]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6))
}

class RoutineOps7(routine: BQRoutine[_7]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7))
}

class RoutineOps8(routine: BQRoutine[_8]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8))
}

class RoutineOps9(routine: BQRoutine[_9]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet,
      m9: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9))
}

class RoutineOps10(routine: BQRoutine[_10]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet,
      m9: BQSqlFrag.Magnet,
      m10: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10))
}

class RoutineOps11(routine: BQRoutine[_11]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet,
      m9: BQSqlFrag.Magnet,
      m10: BQSqlFrag.Magnet,
      m11: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11))
}

class RoutineOps12(routine: BQRoutine[_12]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet,
      m9: BQSqlFrag.Magnet,
      m10: BQSqlFrag.Magnet,
      m11: BQSqlFrag.Magnet,
      m12: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12))
}

class RoutineOps13(routine: BQRoutine[_13]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet,
      m9: BQSqlFrag.Magnet,
      m10: BQSqlFrag.Magnet,
      m11: BQSqlFrag.Magnet,
      m12: BQSqlFrag.Magnet,
      m13: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13))
}

class RoutineOps14(routine: BQRoutine[_14]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet,
      m9: BQSqlFrag.Magnet,
      m10: BQSqlFrag.Magnet,
      m11: BQSqlFrag.Magnet,
      m12: BQSqlFrag.Magnet,
      m13: BQSqlFrag.Magnet,
      m14: BQSqlFrag.Magnet
  ): BQSqlFrag =
    routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14))
}

class RoutineOps15(routine: BQRoutine[_15]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet,
      m9: BQSqlFrag.Magnet,
      m10: BQSqlFrag.Magnet,
      m11: BQSqlFrag.Magnet,
      m12: BQSqlFrag.Magnet,
      m13: BQSqlFrag.Magnet,
      m14: BQSqlFrag.Magnet,
      m15: BQSqlFrag.Magnet
  ): BQSqlFrag =
    routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15))
}

class RoutineOps16(routine: BQRoutine[_16]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet,
      m9: BQSqlFrag.Magnet,
      m10: BQSqlFrag.Magnet,
      m11: BQSqlFrag.Magnet,
      m12: BQSqlFrag.Magnet,
      m13: BQSqlFrag.Magnet,
      m14: BQSqlFrag.Magnet,
      m15: BQSqlFrag.Magnet,
      m16: BQSqlFrag.Magnet
  ): BQSqlFrag =
    routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16))
}

class RoutineOps17(routine: BQRoutine[_17]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet,
      m9: BQSqlFrag.Magnet,
      m10: BQSqlFrag.Magnet,
      m11: BQSqlFrag.Magnet,
      m12: BQSqlFrag.Magnet,
      m13: BQSqlFrag.Magnet,
      m14: BQSqlFrag.Magnet,
      m15: BQSqlFrag.Magnet,
      m16: BQSqlFrag.Magnet,
      m17: BQSqlFrag.Magnet
  ): BQSqlFrag =
    routine.call(
      RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17))
}

class RoutineOps18(routine: BQRoutine[_18]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet,
      m9: BQSqlFrag.Magnet,
      m10: BQSqlFrag.Magnet,
      m11: BQSqlFrag.Magnet,
      m12: BQSqlFrag.Magnet,
      m13: BQSqlFrag.Magnet,
      m14: BQSqlFrag.Magnet,
      m15: BQSqlFrag.Magnet,
      m16: BQSqlFrag.Magnet,
      m17: BQSqlFrag.Magnet,
      m18: BQSqlFrag.Magnet
  ): BQSqlFrag =
    routine.call(
      RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18))
}

class RoutineOps19(routine: BQRoutine[_19]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet,
      m9: BQSqlFrag.Magnet,
      m10: BQSqlFrag.Magnet,
      m11: BQSqlFrag.Magnet,
      m12: BQSqlFrag.Magnet,
      m13: BQSqlFrag.Magnet,
      m14: BQSqlFrag.Magnet,
      m15: BQSqlFrag.Magnet,
      m16: BQSqlFrag.Magnet,
      m17: BQSqlFrag.Magnet,
      m18: BQSqlFrag.Magnet,
      m19: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(
    RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19))
}

class RoutineOps20(routine: BQRoutine[_20]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet,
      m9: BQSqlFrag.Magnet,
      m10: BQSqlFrag.Magnet,
      m11: BQSqlFrag.Magnet,
      m12: BQSqlFrag.Magnet,
      m13: BQSqlFrag.Magnet,
      m14: BQSqlFrag.Magnet,
      m15: BQSqlFrag.Magnet,
      m16: BQSqlFrag.Magnet,
      m17: BQSqlFrag.Magnet,
      m18: BQSqlFrag.Magnet,
      m19: BQSqlFrag.Magnet,
      m20: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(
    RoutineSyntax.builder
      .apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20))
}

class RoutineOps21(routine: BQRoutine[_21]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet,
      m9: BQSqlFrag.Magnet,
      m10: BQSqlFrag.Magnet,
      m11: BQSqlFrag.Magnet,
      m12: BQSqlFrag.Magnet,
      m13: BQSqlFrag.Magnet,
      m14: BQSqlFrag.Magnet,
      m15: BQSqlFrag.Magnet,
      m16: BQSqlFrag.Magnet,
      m17: BQSqlFrag.Magnet,
      m18: BQSqlFrag.Magnet,
      m19: BQSqlFrag.Magnet,
      m20: BQSqlFrag.Magnet,
      m21: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(
    RoutineSyntax.builder
      .apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21))
}

class RoutineOps22(routine: BQRoutine[_22]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet,
      m9: BQSqlFrag.Magnet,
      m10: BQSqlFrag.Magnet,
      m11: BQSqlFrag.Magnet,
      m12: BQSqlFrag.Magnet,
      m13: BQSqlFrag.Magnet,
      m14: BQSqlFrag.Magnet,
      m15: BQSqlFrag.Magnet,
      m16: BQSqlFrag.Magnet,
      m17: BQSqlFrag.Magnet,
      m18: BQSqlFrag.Magnet,
      m19: BQSqlFrag.Magnet,
      m20: BQSqlFrag.Magnet,
      m21: BQSqlFrag.Magnet,
      m22: BQSqlFrag.Magnet
  ): BQSqlFrag = routine.call(
    RoutineSyntax.builder
      .apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22))
}
