/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package no.nrk.bigquery

import org.apache.avro
import org.apache.avro.generic.GenericRecord
import magnolia1.{CaseClass, ProductDerivation}
import scala.deriving.Mirror

private[bigquery] trait BQReadCompat extends ProductDerivation[BQRead] {
  self: BQRead.type =>
  // magnolia automatic derivation begin
  type Typeclass[T] = BQRead[T]

  def join[T](ctx: CaseClass[BQRead, T]): BQRead[T] =
    new BQRead[T] {
      override val bqType: BQType =
        BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          ctx.params.map(param => param.label -> param.typeclass.bqType).toList
        )

      override def read(transportSchema: avro.Schema, value: Any): T =
        value match {
          case coll: GenericRecord =>
            // ignore if BQ thinks this record type is nullable if we think it's not
            val schema1 =
              firstNotNullable(transportSchema).getOrElse(transportSchema)
            val fields = schema1.getFields
            ctx.construct { param =>
              param.typeclass.read(
                fields.get(param.index).schema(),
                coll.get(param.index)
              )
            }
          case other =>
            sys.error(
              s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema"
            )
        }
    }

  inline given [A <: Tuple](using m: Mirror.Of[A]): BQRead[A] = derived

  given convertsTuple[A: BQRead, B: BQRead]: BQRead[(A, B)] = derived[(A, B)]
}
