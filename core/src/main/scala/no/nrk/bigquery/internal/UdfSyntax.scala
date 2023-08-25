package no.nrk.bigquery.internal

import no.nrk.bigquery.{BQSqlFrag, UDF}
import shapeless.nat._

trait UdfSyntax {
  implicit def bqUdfOps0(udf: UDF[UDF.UDFId, _0]): UdfOps0 = new UdfOps0(udf)
  implicit def bqUdfOps1(udf: UDF[UDF.UDFId, _1]): UdfOps1 = new UdfOps1(udf)
  implicit def bqUdfOps2(udf: UDF[UDF.UDFId, _2]): UdfOps2 = new UdfOps2(udf)
  implicit def bqUdfOps3(udf: UDF[UDF.UDFId, _3]): UdfOps3 = new UdfOps3(udf)
  implicit def bqUdfOps4(udf: UDF[UDF.UDFId, _4]): UdfOps4 = new UdfOps4(udf)
  implicit def bqUdfOps5(udf: UDF[UDF.UDFId, _5]): UdfOps5 = new UdfOps5(udf)
  implicit def bqUdfOps6(udf: UDF[UDF.UDFId, _6]): UdfOps6 = new UdfOps6(udf)
  implicit def bqUdfOps7(udf: UDF[UDF.UDFId, _7]): UdfOps7 = new UdfOps7(udf)
  implicit def bqUdfOps8(udf: UDF[UDF.UDFId, _8]): UdfOps8 = new UdfOps8(udf)
  implicit def bqUdfOps9(udf: UDF[UDF.UDFId, _9]): UdfOps9 = new UdfOps9(udf)
  implicit def bqUdfOps10(udf: UDF[UDF.UDFId, _10]): UdfOps10 = new UdfOps10(udf)
  implicit def bqUdfOps11(udf: UDF[UDF.UDFId, _11]): UdfOps11 = new UdfOps11(udf)
  implicit def bqUdfOps12(udf: UDF[UDF.UDFId, _12]): UdfOps12 = new UdfOps12(udf)
  implicit def bqUdfOps13(udf: UDF[UDF.UDFId, _13]): UdfOps13 = new UdfOps13(udf)
  implicit def bqUdfOps14(udf: UDF[UDF.UDFId, _14]): UdfOps14 = new UdfOps14(udf)
  implicit def bqUdfOps15(udf: UDF[UDF.UDFId, _15]): UdfOps15 = new UdfOps15(udf)
  implicit def bqUdfOps16(udf: UDF[UDF.UDFId, _16]): UdfOps16 = new UdfOps16(udf)
  implicit def bqUdfOps17(udf: UDF[UDF.UDFId, _17]): UdfOps17 = new UdfOps17(udf)
  implicit def bqUdfOps18(udf: UDF[UDF.UDFId, _18]): UdfOps18 = new UdfOps18(udf)
  implicit def bqUdfOps19(udf: UDF[UDF.UDFId, _19]): UdfOps19 = new UdfOps19(udf)
  implicit def bqUdfOps20(udf: UDF[UDF.UDFId, _20]): UdfOps20 = new UdfOps20(udf)
  implicit def bqUdfOps21(udf: UDF[UDF.UDFId, _21]): UdfOps21 = new UdfOps21(udf)
  implicit def bqUdfOps22(udf: UDF[UDF.UDFId, _22]): UdfOps22 = new UdfOps22(udf)
}

class UdfOps0(udf: UDF[UDF.UDFId, _0]) {
  def apply(): BQSqlFrag.Call = BQSqlFrag.Call(udf, List.empty)
}

class UdfOps1(udf: UDF[UDF.UDFId, _1]) {
  def apply(
      m1: BQSqlFrag.Magnet
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1
    ).map(_.frag))
}

class UdfOps2(udf: UDF[UDF.UDFId, _2]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2
    ).map(_.frag))
}

class UdfOps3(udf: UDF[UDF.UDFId, _3]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3
    ).map(_.frag))
}

class UdfOps4(udf: UDF[UDF.UDFId, _4]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4
    ).map(_.frag))
}

class UdfOps5(udf: UDF[UDF.UDFId, _5]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5
    ).map(_.frag))
}

class UdfOps6(udf: UDF[UDF.UDFId, _6]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6
    ).map(_.frag))
}

class UdfOps7(udf: UDF[UDF.UDFId, _7]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6,
      m7
    ).map(_.frag))
}

class UdfOps8(udf: UDF[UDF.UDFId, _8]) {
  def apply(
      m1: BQSqlFrag.Magnet,
      m2: BQSqlFrag.Magnet,
      m3: BQSqlFrag.Magnet,
      m4: BQSqlFrag.Magnet,
      m5: BQSqlFrag.Magnet,
      m6: BQSqlFrag.Magnet,
      m7: BQSqlFrag.Magnet,
      m8: BQSqlFrag.Magnet
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6,
      m7,
      m8
    ).map(_.frag))
}

class UdfOps9(udf: UDF[UDF.UDFId, _9]) {
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
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6,
      m7,
      m8,
      m9
    ).map(_.frag))
}

class UdfOps10(udf: UDF[UDF.UDFId, _10]) {
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
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6,
      m7,
      m8,
      m9,
      m10
    ).map(_.frag))
}

class UdfOps11(udf: UDF[UDF.UDFId, _11]) {
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
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6,
      m7,
      m8,
      m9,
      m10,
      m11
    ).map(_.frag))
}

class UdfOps12(udf: UDF[UDF.UDFId, _12]) {
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
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6,
      m7,
      m8,
      m9,
      m10,
      m11,
      m12
    ).map(_.frag))
}

class UdfOps13(udf: UDF[UDF.UDFId, _13]) {
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
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6,
      m7,
      m8,
      m9,
      m10,
      m11,
      m12,
      m13
    ).map(_.frag))
}

class UdfOps14(udf: UDF[UDF.UDFId, _14]) {
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
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6,
      m7,
      m8,
      m9,
      m10,
      m11,
      m12,
      m13,
      m14
    ).map(_.frag))
}

class UdfOps15(udf: UDF[UDF.UDFId, _15]) {
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
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6,
      m7,
      m8,
      m9,
      m10,
      m11,
      m12,
      m13,
      m14,
      m15
    ).map(_.frag))
}

class UdfOps16(udf: UDF[UDF.UDFId, _16]) {
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
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6,
      m7,
      m8,
      m9,
      m10,
      m11,
      m12,
      m13,
      m14,
      m15,
      m16
    ).map(_.frag))
}

class UdfOps17(udf: UDF[UDF.UDFId, _17]) {
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
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6,
      m7,
      m8,
      m9,
      m10,
      m11,
      m12,
      m13,
      m14,
      m15,
      m16,
      m17
    ).map(_.frag))
}

class UdfOps18(udf: UDF[UDF.UDFId, _18]) {
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
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6,
      m7,
      m8,
      m9,
      m10,
      m11,
      m12,
      m13,
      m14,
      m15,
      m16,
      m17,
      m18
    ).map(_.frag))
}

class UdfOps19(udf: UDF[UDF.UDFId, _19]) {
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
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6,
      m7,
      m8,
      m9,
      m10,
      m11,
      m12,
      m13,
      m14,
      m15,
      m16,
      m17,
      m18,
      m19
    ).map(_.frag))
}

class UdfOps20(udf: UDF[UDF.UDFId, _20]) {
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
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6,
      m7,
      m8,
      m9,
      m10,
      m11,
      m12,
      m13,
      m14,
      m15,
      m16,
      m17,
      m18,
      m19,
      m20
    ).map(_.frag))
}

class UdfOps21(udf: UDF[UDF.UDFId, _21]) {
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
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6,
      m7,
      m8,
      m9,
      m10,
      m11,
      m12,
      m13,
      m14,
      m15,
      m16,
      m17,
      m18,
      m19,
      m20,
      m21
    ).map(_.frag))
}

class UdfOps22(udf: UDF[UDF.UDFId, _22]) {
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
  ): BQSqlFrag.Call = BQSqlFrag.Call(
    udf,
    List(
      m1,
      m2,
      m3,
      m4,
      m5,
      m6,
      m7,
      m8,
      m9,
      m10,
      m11,
      m12,
      m13,
      m14,
      m15,
      m16,
      m17,
      m18,
      m19,
      m20,
      m21,
      m22
    ).map(_.frag))
}
