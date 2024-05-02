// auto-generated boilerplate
package no.nrk.bigquery

import no.nrk.bigquery.BQRead.firstNotNullable
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

private[bigquery] trait ProductBQRead {
  /**
   * @group Product
   */
  final def forProduct1[Target, A0](nameA0: String)(f: (A0) => Target)(implicit
    bqReadA0: BQRead[A0]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct2[Target, A0, A1](nameA0: String, nameA1: String)(f: (A0, A1) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct3[Target, A0, A1, A2](nameA0: String, nameA1: String, nameA2: String)(f: (A0, A1, A2) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct4[Target, A0, A1, A2, A3](nameA0: String, nameA1: String, nameA2: String, nameA3: String)(f: (A0, A1, A2, A3) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct5[Target, A0, A1, A2, A3, A4](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String)(f: (A0, A1, A2, A3, A4) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct6[Target, A0, A1, A2, A3, A4, A5](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String)(f: (A0, A1, A2, A3, A4, A5) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct7[Target, A0, A1, A2, A3, A4, A5, A6](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String, nameA6: String)(f: (A0, A1, A2, A3, A4, A5, A6) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5], bqReadA6: BQRead[A6]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType, nameA6 -> bqReadA6.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5), getField[A6](6))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct8[Target, A0, A1, A2, A3, A4, A5, A6, A7](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String, nameA6: String, nameA7: String)(f: (A0, A1, A2, A3, A4, A5, A6, A7) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5], bqReadA6: BQRead[A6], bqReadA7: BQRead[A7]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType, nameA6 -> bqReadA6.bqType, nameA7 -> bqReadA7.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5), getField[A6](6), getField[A7](7))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct9[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String, nameA6: String, nameA7: String, nameA8: String)(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5], bqReadA6: BQRead[A6], bqReadA7: BQRead[A7], bqReadA8: BQRead[A8]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType, nameA6 -> bqReadA6.bqType, nameA7 -> bqReadA7.bqType, nameA8 -> bqReadA8.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5), getField[A6](6), getField[A7](7), getField[A8](8))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct10[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String, nameA6: String, nameA7: String, nameA8: String, nameA9: String)(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5], bqReadA6: BQRead[A6], bqReadA7: BQRead[A7], bqReadA8: BQRead[A8], bqReadA9: BQRead[A9]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType, nameA6 -> bqReadA6.bqType, nameA7 -> bqReadA7.bqType, nameA8 -> bqReadA8.bqType, nameA9 -> bqReadA9.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5), getField[A6](6), getField[A7](7), getField[A8](8), getField[A9](9))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct11[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String, nameA6: String, nameA7: String, nameA8: String, nameA9: String, nameA10: String)(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5], bqReadA6: BQRead[A6], bqReadA7: BQRead[A7], bqReadA8: BQRead[A8], bqReadA9: BQRead[A9], bqReadA10: BQRead[A10]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType, nameA6 -> bqReadA6.bqType, nameA7 -> bqReadA7.bqType, nameA8 -> bqReadA8.bqType, nameA9 -> bqReadA9.bqType, nameA10 -> bqReadA10.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5), getField[A6](6), getField[A7](7), getField[A8](8), getField[A9](9), getField[A10](10))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct12[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String, nameA6: String, nameA7: String, nameA8: String, nameA9: String, nameA10: String, nameA11: String)(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5], bqReadA6: BQRead[A6], bqReadA7: BQRead[A7], bqReadA8: BQRead[A8], bqReadA9: BQRead[A9], bqReadA10: BQRead[A10], bqReadA11: BQRead[A11]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType, nameA6 -> bqReadA6.bqType, nameA7 -> bqReadA7.bqType, nameA8 -> bqReadA8.bqType, nameA9 -> bqReadA9.bqType, nameA10 -> bqReadA10.bqType, nameA11 -> bqReadA11.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5), getField[A6](6), getField[A7](7), getField[A8](8), getField[A9](9), getField[A10](10), getField[A11](11))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct13[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String, nameA6: String, nameA7: String, nameA8: String, nameA9: String, nameA10: String, nameA11: String, nameA12: String)(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5], bqReadA6: BQRead[A6], bqReadA7: BQRead[A7], bqReadA8: BQRead[A8], bqReadA9: BQRead[A9], bqReadA10: BQRead[A10], bqReadA11: BQRead[A11], bqReadA12: BQRead[A12]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType, nameA6 -> bqReadA6.bqType, nameA7 -> bqReadA7.bqType, nameA8 -> bqReadA8.bqType, nameA9 -> bqReadA9.bqType, nameA10 -> bqReadA10.bqType, nameA11 -> bqReadA11.bqType, nameA12 -> bqReadA12.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5), getField[A6](6), getField[A7](7), getField[A8](8), getField[A9](9), getField[A10](10), getField[A11](11), getField[A12](12))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct14[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String, nameA6: String, nameA7: String, nameA8: String, nameA9: String, nameA10: String, nameA11: String, nameA12: String, nameA13: String)(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5], bqReadA6: BQRead[A6], bqReadA7: BQRead[A7], bqReadA8: BQRead[A8], bqReadA9: BQRead[A9], bqReadA10: BQRead[A10], bqReadA11: BQRead[A11], bqReadA12: BQRead[A12], bqReadA13: BQRead[A13]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType, nameA6 -> bqReadA6.bqType, nameA7 -> bqReadA7.bqType, nameA8 -> bqReadA8.bqType, nameA9 -> bqReadA9.bqType, nameA10 -> bqReadA10.bqType, nameA11 -> bqReadA11.bqType, nameA12 -> bqReadA12.bqType, nameA13 -> bqReadA13.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5), getField[A6](6), getField[A7](7), getField[A8](8), getField[A9](9), getField[A10](10), getField[A11](11), getField[A12](12), getField[A13](13))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct15[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String, nameA6: String, nameA7: String, nameA8: String, nameA9: String, nameA10: String, nameA11: String, nameA12: String, nameA13: String, nameA14: String)(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5], bqReadA6: BQRead[A6], bqReadA7: BQRead[A7], bqReadA8: BQRead[A8], bqReadA9: BQRead[A9], bqReadA10: BQRead[A10], bqReadA11: BQRead[A11], bqReadA12: BQRead[A12], bqReadA13: BQRead[A13], bqReadA14: BQRead[A14]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType, nameA6 -> bqReadA6.bqType, nameA7 -> bqReadA7.bqType, nameA8 -> bqReadA8.bqType, nameA9 -> bqReadA9.bqType, nameA10 -> bqReadA10.bqType, nameA11 -> bqReadA11.bqType, nameA12 -> bqReadA12.bqType, nameA13 -> bqReadA13.bqType, nameA14 -> bqReadA14.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5), getField[A6](6), getField[A7](7), getField[A8](8), getField[A9](9), getField[A10](10), getField[A11](11), getField[A12](12), getField[A13](13), getField[A14](14))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct16[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String, nameA6: String, nameA7: String, nameA8: String, nameA9: String, nameA10: String, nameA11: String, nameA12: String, nameA13: String, nameA14: String, nameA15: String)(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5], bqReadA6: BQRead[A6], bqReadA7: BQRead[A7], bqReadA8: BQRead[A8], bqReadA9: BQRead[A9], bqReadA10: BQRead[A10], bqReadA11: BQRead[A11], bqReadA12: BQRead[A12], bqReadA13: BQRead[A13], bqReadA14: BQRead[A14], bqReadA15: BQRead[A15]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType, nameA6 -> bqReadA6.bqType, nameA7 -> bqReadA7.bqType, nameA8 -> bqReadA8.bqType, nameA9 -> bqReadA9.bqType, nameA10 -> bqReadA10.bqType, nameA11 -> bqReadA11.bqType, nameA12 -> bqReadA12.bqType, nameA13 -> bqReadA13.bqType, nameA14 -> bqReadA14.bqType, nameA15 -> bqReadA15.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5), getField[A6](6), getField[A7](7), getField[A8](8), getField[A9](9), getField[A10](10), getField[A11](11), getField[A12](12), getField[A13](13), getField[A14](14), getField[A15](15))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct17[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String, nameA6: String, nameA7: String, nameA8: String, nameA9: String, nameA10: String, nameA11: String, nameA12: String, nameA13: String, nameA14: String, nameA15: String, nameA16: String)(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5], bqReadA6: BQRead[A6], bqReadA7: BQRead[A7], bqReadA8: BQRead[A8], bqReadA9: BQRead[A9], bqReadA10: BQRead[A10], bqReadA11: BQRead[A11], bqReadA12: BQRead[A12], bqReadA13: BQRead[A13], bqReadA14: BQRead[A14], bqReadA15: BQRead[A15], bqReadA16: BQRead[A16]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType, nameA6 -> bqReadA6.bqType, nameA7 -> bqReadA7.bqType, nameA8 -> bqReadA8.bqType, nameA9 -> bqReadA9.bqType, nameA10 -> bqReadA10.bqType, nameA11 -> bqReadA11.bqType, nameA12 -> bqReadA12.bqType, nameA13 -> bqReadA13.bqType, nameA14 -> bqReadA14.bqType, nameA15 -> bqReadA15.bqType, nameA16 -> bqReadA16.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5), getField[A6](6), getField[A7](7), getField[A8](8), getField[A9](9), getField[A10](10), getField[A11](11), getField[A12](12), getField[A13](13), getField[A14](14), getField[A15](15), getField[A16](16))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct18[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String, nameA6: String, nameA7: String, nameA8: String, nameA9: String, nameA10: String, nameA11: String, nameA12: String, nameA13: String, nameA14: String, nameA15: String, nameA16: String, nameA17: String)(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5], bqReadA6: BQRead[A6], bqReadA7: BQRead[A7], bqReadA8: BQRead[A8], bqReadA9: BQRead[A9], bqReadA10: BQRead[A10], bqReadA11: BQRead[A11], bqReadA12: BQRead[A12], bqReadA13: BQRead[A13], bqReadA14: BQRead[A14], bqReadA15: BQRead[A15], bqReadA16: BQRead[A16], bqReadA17: BQRead[A17]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType, nameA6 -> bqReadA6.bqType, nameA7 -> bqReadA7.bqType, nameA8 -> bqReadA8.bqType, nameA9 -> bqReadA9.bqType, nameA10 -> bqReadA10.bqType, nameA11 -> bqReadA11.bqType, nameA12 -> bqReadA12.bqType, nameA13 -> bqReadA13.bqType, nameA14 -> bqReadA14.bqType, nameA15 -> bqReadA15.bqType, nameA16 -> bqReadA16.bqType, nameA17 -> bqReadA17.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5), getField[A6](6), getField[A7](7), getField[A8](8), getField[A9](9), getField[A10](10), getField[A11](11), getField[A12](12), getField[A13](13), getField[A14](14), getField[A15](15), getField[A16](16), getField[A17](17))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct19[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String, nameA6: String, nameA7: String, nameA8: String, nameA9: String, nameA10: String, nameA11: String, nameA12: String, nameA13: String, nameA14: String, nameA15: String, nameA16: String, nameA17: String, nameA18: String)(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5], bqReadA6: BQRead[A6], bqReadA7: BQRead[A7], bqReadA8: BQRead[A8], bqReadA9: BQRead[A9], bqReadA10: BQRead[A10], bqReadA11: BQRead[A11], bqReadA12: BQRead[A12], bqReadA13: BQRead[A13], bqReadA14: BQRead[A14], bqReadA15: BQRead[A15], bqReadA16: BQRead[A16], bqReadA17: BQRead[A17], bqReadA18: BQRead[A18]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType, nameA6 -> bqReadA6.bqType, nameA7 -> bqReadA7.bqType, nameA8 -> bqReadA8.bqType, nameA9 -> bqReadA9.bqType, nameA10 -> bqReadA10.bqType, nameA11 -> bqReadA11.bqType, nameA12 -> bqReadA12.bqType, nameA13 -> bqReadA13.bqType, nameA14 -> bqReadA14.bqType, nameA15 -> bqReadA15.bqType, nameA16 -> bqReadA16.bqType, nameA17 -> bqReadA17.bqType, nameA18 -> bqReadA18.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5), getField[A6](6), getField[A7](7), getField[A8](8), getField[A9](9), getField[A10](10), getField[A11](11), getField[A12](12), getField[A13](13), getField[A14](14), getField[A15](15), getField[A16](16), getField[A17](17), getField[A18](18))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct20[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String, nameA6: String, nameA7: String, nameA8: String, nameA9: String, nameA10: String, nameA11: String, nameA12: String, nameA13: String, nameA14: String, nameA15: String, nameA16: String, nameA17: String, nameA18: String, nameA19: String)(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5], bqReadA6: BQRead[A6], bqReadA7: BQRead[A7], bqReadA8: BQRead[A8], bqReadA9: BQRead[A9], bqReadA10: BQRead[A10], bqReadA11: BQRead[A11], bqReadA12: BQRead[A12], bqReadA13: BQRead[A13], bqReadA14: BQRead[A14], bqReadA15: BQRead[A15], bqReadA16: BQRead[A16], bqReadA17: BQRead[A17], bqReadA18: BQRead[A18], bqReadA19: BQRead[A19]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType, nameA6 -> bqReadA6.bqType, nameA7 -> bqReadA7.bqType, nameA8 -> bqReadA8.bqType, nameA9 -> bqReadA9.bqType, nameA10 -> bqReadA10.bqType, nameA11 -> bqReadA11.bqType, nameA12 -> bqReadA12.bqType, nameA13 -> bqReadA13.bqType, nameA14 -> bqReadA14.bqType, nameA15 -> bqReadA15.bqType, nameA16 -> bqReadA16.bqType, nameA17 -> bqReadA17.bqType, nameA18 -> bqReadA18.bqType, nameA19 -> bqReadA19.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5), getField[A6](6), getField[A7](7), getField[A8](8), getField[A9](9), getField[A10](10), getField[A11](11), getField[A12](12), getField[A13](13), getField[A14](14), getField[A15](15), getField[A16](16), getField[A17](17), getField[A18](18), getField[A19](19))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct21[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String, nameA6: String, nameA7: String, nameA8: String, nameA9: String, nameA10: String, nameA11: String, nameA12: String, nameA13: String, nameA14: String, nameA15: String, nameA16: String, nameA17: String, nameA18: String, nameA19: String, nameA20: String)(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5], bqReadA6: BQRead[A6], bqReadA7: BQRead[A7], bqReadA8: BQRead[A8], bqReadA9: BQRead[A9], bqReadA10: BQRead[A10], bqReadA11: BQRead[A11], bqReadA12: BQRead[A12], bqReadA13: BQRead[A13], bqReadA14: BQRead[A14], bqReadA15: BQRead[A15], bqReadA16: BQRead[A16], bqReadA17: BQRead[A17], bqReadA18: BQRead[A18], bqReadA19: BQRead[A19], bqReadA20: BQRead[A20]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType, nameA6 -> bqReadA6.bqType, nameA7 -> bqReadA7.bqType, nameA8 -> bqReadA8.bqType, nameA9 -> bqReadA9.bqType, nameA10 -> bqReadA10.bqType, nameA11 -> bqReadA11.bqType, nameA12 -> bqReadA12.bqType, nameA13 -> bqReadA13.bqType, nameA14 -> bqReadA14.bqType, nameA15 -> bqReadA15.bqType, nameA16 -> bqReadA16.bqType, nameA17 -> bqReadA17.bqType, nameA18 -> bqReadA18.bqType, nameA19 -> bqReadA19.bqType, nameA20 -> bqReadA20.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5), getField[A6](6), getField[A7](7), getField[A8](8), getField[A9](9), getField[A10](10), getField[A11](11), getField[A12](12), getField[A13](13), getField[A14](14), getField[A15](15), getField[A16](16), getField[A17](17), getField[A18](18), getField[A19](19), getField[A20](20))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
  /**
   * @group Product
   */
  final def forProduct22[Target, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21](nameA0: String, nameA1: String, nameA2: String, nameA3: String, nameA4: String, nameA5: String, nameA6: String, nameA7: String, nameA8: String, nameA9: String, nameA10: String, nameA11: String, nameA12: String, nameA13: String, nameA14: String, nameA15: String, nameA16: String, nameA17: String, nameA18: String, nameA19: String, nameA20: String, nameA21: String)(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21) => Target)(implicit
    bqReadA0: BQRead[A0], bqReadA1: BQRead[A1], bqReadA2: BQRead[A2], bqReadA3: BQRead[A3], bqReadA4: BQRead[A4], bqReadA5: BQRead[A5], bqReadA6: BQRead[A6], bqReadA7: BQRead[A7], bqReadA8: BQRead[A8], bqReadA9: BQRead[A9], bqReadA10: BQRead[A10], bqReadA11: BQRead[A11], bqReadA12: BQRead[A12], bqReadA13: BQRead[A13], bqReadA14: BQRead[A14], bqReadA15: BQRead[A15], bqReadA16: BQRead[A16], bqReadA17: BQRead[A17], bqReadA18: BQRead[A18], bqReadA19: BQRead[A19], bqReadA20: BQRead[A20], bqReadA21: BQRead[A21]
  ): BQRead[Target] = new BQRead[Target] {
      override val bqType: BQType = BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          List(nameA0 -> bqReadA0.bqType, nameA1 -> bqReadA1.bqType, nameA2 -> bqReadA2.bqType, nameA3 -> bqReadA3.bqType, nameA4 -> bqReadA4.bqType, nameA5 -> bqReadA5.bqType, nameA6 -> bqReadA6.bqType, nameA7 -> bqReadA7.bqType, nameA8 -> bqReadA8.bqType, nameA9 -> bqReadA9.bqType, nameA10 -> bqReadA10.bqType, nameA11 -> bqReadA11.bqType, nameA12 -> bqReadA12.bqType, nameA13 -> bqReadA13.bqType, nameA14 -> bqReadA14.bqType, nameA15 -> bqReadA15.bqType, nameA16 -> bqReadA16.bqType, nameA17 -> bqReadA17.bqType, nameA18 -> bqReadA18.bqType, nameA19 -> bqReadA19.bqType, nameA20 -> bqReadA20.bqType, nameA21 -> bqReadA21.bqType)
       )
      override def read(transportSchema: Schema, value: Any): Target =
        value match {
          case coll: GenericRecord =>
            val fields = firstNotNullable(transportSchema).getOrElse(transportSchema).getFields
            def getField[A: BQRead](index: Int) : A = implicitly[BQRead[A]].read(fields.get(index).schema(), coll.get(index))
            f(getField[A0](0), getField[A1](1), getField[A2](2), getField[A3](3), getField[A4](4), getField[A5](5), getField[A6](6), getField[A7](7), getField[A8](8), getField[A9](9), getField[A10](10), getField[A11](11), getField[A12](12), getField[A13](13), getField[A14](14), getField[A15](15), getField[A16](16), getField[A17](17), getField[A18](18), getField[A19](19), getField[A20](20), getField[A21](21))
          case other => sys.error(s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema")
        }
    }
}