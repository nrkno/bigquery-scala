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
  implicit def bqRoutineOps0[C](routine: BQRoutine[_0, C]): RoutineOps0[C] = new RoutineOps0(routine)
  implicit def bqRoutineOps1[C](routine: BQRoutine[_1, C]): RoutineOps1[C] = new RoutineOps1(routine)
  implicit def bqRoutineOps2[C](routine: BQRoutine[_2, C]): RoutineOps2[C] = new RoutineOps2(routine)
  implicit def bqRoutineOps3[C](routine: BQRoutine[_3, C]): RoutineOps3[C] = new RoutineOps3(routine)
  implicit def bqRoutineOps4[C](routine: BQRoutine[_4, C]): RoutineOps4[C] = new RoutineOps4(routine)
  implicit def bqRoutineOps5[C](routine: BQRoutine[_5, C]): RoutineOps5[C] = new RoutineOps5(routine)
  implicit def bqRoutineOps6[C](routine: BQRoutine[_6, C]): RoutineOps6[C] = new RoutineOps6(routine)
  implicit def bqRoutineOps7[C](routine: BQRoutine[_7, C]): RoutineOps7[C] = new RoutineOps7(routine)
  implicit def bqRoutineOps8[C](routine: BQRoutine[_8, C]): RoutineOps8[C] = new RoutineOps8(routine)
  implicit def bqRoutineOps9[C](routine: BQRoutine[_9, C]): RoutineOps9[C] = new RoutineOps9(routine)
  implicit def bqRoutineOps10[C](routine: BQRoutine[_10, C]): RoutineOps10[C] = new RoutineOps10(routine)
  implicit def bqRoutineOps11[C](routine: BQRoutine[_11, C]): RoutineOps11[C] = new RoutineOps11(routine)
  implicit def bqRoutineOps12[C](routine: BQRoutine[_12, C]): RoutineOps12[C] = new RoutineOps12(routine)
  implicit def bqRoutineOps13[C](routine: BQRoutine[_13, C]): RoutineOps13[C] = new RoutineOps13(routine)
  implicit def bqRoutineOps14[C](routine: BQRoutine[_14, C]): RoutineOps14[C] = new RoutineOps14(routine)
  implicit def bqRoutineOps15[C](routine: BQRoutine[_15, C]): RoutineOps15[C] = new RoutineOps15(routine)
  implicit def bqRoutineOps16[C](routine: BQRoutine[_16, C]): RoutineOps16[C] = new RoutineOps16(routine)
  implicit def bqRoutineOps17[C](routine: BQRoutine[_17, C]): RoutineOps17[C] = new RoutineOps17(routine)
  implicit def bqRoutineOps18[C](routine: BQRoutine[_18, C]): RoutineOps18[C] = new RoutineOps18(routine)
  implicit def bqRoutineOps19[C](routine: BQRoutine[_19, C]): RoutineOps19[C] = new RoutineOps19(routine)
  implicit def bqRoutineOps20[C](routine: BQRoutine[_20, C]): RoutineOps20[C] = new RoutineOps20(routine)
  implicit def bqRoutineOps21[C](routine: BQRoutine[_21, C]): RoutineOps21[C] = new RoutineOps21(routine)
  implicit def bqRoutineOps22[C](routine: BQRoutine[_22, C]): RoutineOps22[C] = new RoutineOps22(routine)
}

object RoutineSyntax {
  private[internal] val builder = new IndexSeqSizedBuilder[BQSqlFrag.Magnet]
}

class RoutineOps0[C](routine: BQRoutine[_0, C]) {
  def apply(): C =
    routine.call(RoutineSyntax.builder.empty)
}

class RoutineOps1[C](routine: BQRoutine[_1, C]) {
  def apply(
      m1: BQSqlFrag.Magnet
  ): C = routine.call(RoutineSyntax.builder.apply(m1))
}

class RoutineOps2[C](routine: BQRoutine[_2, C]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet
  ): C = routine.call(RoutineSyntax.builder.apply(m1, m2))
}

class RoutineOps3[C](routine: BQRoutine[_3, C]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet
  ): C = routine.call(RoutineSyntax.builder.apply(m1, m2, m3))
}

class RoutineOps4[C](routine: BQRoutine[_4, C]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet
  ): C = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4))
}

class RoutineOps5[C](routine: BQRoutine[_5, C]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet
  ): C = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5))
}

class RoutineOps6[C](routine: BQRoutine[_6, C]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet
  ): C = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6))
}

class RoutineOps7[C](routine: BQRoutine[_7, C]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet
  ): C = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7))
}

class RoutineOps8[C](routine: BQRoutine[_8, C]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet
  ): C = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8))
}

class RoutineOps9[C](routine: BQRoutine[_9, C]) {
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
  ): C = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9))
}

class RoutineOps10[C](routine: BQRoutine[_10, C]) {
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
  ): C = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10))
}

class RoutineOps11[C](routine: BQRoutine[_11, C]) {
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
  ): C = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11))
}

class RoutineOps12[C](routine: BQRoutine[_12, C]) {
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
  ): C = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12))
}

class RoutineOps13[C](routine: BQRoutine[_13, C]) {
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
  ): C = routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13))
}

class RoutineOps14[C](routine: BQRoutine[_14, C]) {
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
  ): C =
    routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14))
}

class RoutineOps15[C](routine: BQRoutine[_15, C]) {
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
  ): C =
    routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15))
}

class RoutineOps16[C](routine: BQRoutine[_16, C]) {
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
  ): C =
    routine.call(RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16))
}

class RoutineOps17[C](routine: BQRoutine[_17, C]) {
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
  ): C =
    routine.call(
      RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17))
}

class RoutineOps18[C](routine: BQRoutine[_18, C]) {
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
  ): C =
    routine.call(
      RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18))
}

class RoutineOps19[C](routine: BQRoutine[_19, C]) {
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
  ): C = routine.call(
    RoutineSyntax.builder.apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19))
}

class RoutineOps20[C](routine: BQRoutine[_20, C]) {
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
  ): C = routine.call(
    RoutineSyntax.builder
      .apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20))
}

class RoutineOps21[C](routine: BQRoutine[_21, C]) {
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
  ): C = routine.call(
    RoutineSyntax.builder
      .apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21))
}

class RoutineOps22[C](routine: BQRoutine[_22, C]) {
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
  ): C = routine.call(
    RoutineSyntax.builder
      .apply(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22))
}
