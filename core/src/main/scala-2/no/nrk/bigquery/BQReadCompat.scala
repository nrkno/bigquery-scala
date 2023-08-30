package no.nrk.bigquery

import magnolia1.{CaseClass, Magnolia}

import org.apache.avro
import org.apache.avro.generic.GenericRecord
import scala.annotation.nowarn

private[bigquery] trait BQReadCompat { self: BQRead.type =>
  // magnolia automatic derivation begin
  type Typeclass[T] = BQRead[T]

  def join[T](ctx: CaseClass[BQRead, T]): BQRead[T] =
    new BQRead[T] {
      override val bqType: BQType =
        BQType(
          BQField.Mode.REQUIRED,
          BQField.Type.STRUCT,
          ctx.parameters
            .map(param => param.label -> param.typeclass.bqType)
            .toList
        )

      override def read(transportSchema: avro.Schema, value: Any): T =
        value match {
          case coll: GenericRecord =>
            // ignore if BQ thinks this record type is nullable if we think it's not
            val schema1 =
              self.firstNotNullable(transportSchema).getOrElse(transportSchema)
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

  def derived[T]: BQRead[T] = macro Magnolia.gen[T]
  // magnolia automatic derivation end

  @nowarn("cat=unused")
  implicit def convertsTuple[A: BQRead, B: BQRead]: BQRead[(A, B)] =
    derived[(A, B)]

}
