package no.nrk.bigquery

import com.google.cloud.bigquery.{Field, StandardSQLTypeName}
import io.circe.Json
import magnolia1.{CaseClass, Magnolia}
import org.apache.avro
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

import java.time._
import scala.annotation.nowarn
import scala.collection.compat._
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

trait BQRead[A] { outer =>
  final def map[B](f: A => B): BQRead[B] =
    new BQRead[B] {
      override val bqType: BQType =
        outer.bqType
      override def read(transportSchema: avro.Schema, value: Any): B =
        f(outer.read(transportSchema, value))
    }

  val bqType: BQType
  def read(transportSchema: avro.Schema, value: Any): A
}

object BQRead {

  def apply[A: BQRead]: BQRead[A] = implicitly

  // magnolia automatic derivation begin
  type Typeclass[T] = BQRead[T]

  private def firstNotNullable(schema: avro.Schema): Option[avro.Schema] =
    if (schema.isUnion) schema.getTypes.asScala.collectFirst {
      case s if !s.isNullable => s
    }
    else None

  def join[T](ctx: CaseClass[BQRead, T]): BQRead[T] =
    new BQRead[T] {
      override val bqType: BQType =
        BQType(
          Field.Mode.REQUIRED,
          StandardSQLTypeName.STRUCT,
          ctx.parameters
            .map(param => param.label -> param.typeclass.bqType)
            .toList
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

  def derived[T]: BQRead[T] = macro Magnolia.gen[T]
  // magnolia automatic derivation end

  @nowarn("cat=unused")
  implicit def convertsTuple[A: BQRead, B: BQRead]: BQRead[(A, B)] =
    derived[(A, B)]

  implicit def convertsOption[A: BQRead]: BQRead[Option[A]] =
    new BQRead[Option[A]] {
      override val bqType: BQType = {
        val base = BQRead[A].bqType
        if (base.mode == Field.Mode.REQUIRED)
          base.copy(mode = Field.Mode.NULLABLE)
        else base
      }

      override def read(transportSchema: avro.Schema, value: Any): Option[A] =
        if (value == null) None
        else
          Some(
            BQRead[A].read(
              firstNotNullable(transportSchema).getOrElse(transportSchema),
              value
            )
          )
    }

  implicit def convertsIterable[I[t] <: Iterable[t], A: BQRead](implicit
      cb: Factory[A, I[A]]
  ): BQRead[I[A]] =
    new BQRead[I[A]] {
      override val bqType: BQType =
        BQRead[A].bqType.copy(mode = Field.Mode.REPEATED)

      override def read(transportSchema: avro.Schema, value: Any): I[A] = {
        val b = cb.newBuilder
        value match {
          case coll: scala.Array[_] =>
            coll.foreach(elem =>
              b += BQRead[A].read(transportSchema.getElementType, elem)
            )
          case coll: java.util.Collection[_] =>
            coll.forEach(elem =>
              b += BQRead[A].read(transportSchema.getElementType, elem)
            )
          case other =>
            sys.error(
              s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema"
            )
        }
        b.result()
      }
    }

  implicit def convertsArray[A: BQRead: ClassTag]: BQRead[Array[A]] =
    new BQRead[Array[A]] {
      override val bqType: BQType =
        BQRead[A].bqType.copy(mode = Field.Mode.REPEATED)

      override def read(transportSchema: avro.Schema, value: Any): Array[A] = {
        val b = Array.newBuilder[A]
        value match {
          case coll: scala.Array[_] =>
            coll.foreach(elem =>
              b += BQRead[A].read(transportSchema.getElementType, elem)
            )
          case coll: java.util.Collection[_] =>
            coll.forEach(elem =>
              b += BQRead[A].read(transportSchema.getElementType, elem)
            )
          case other =>
            sys.error(
              s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema"
            )
        }
        b.result()
      }
    }

  implicit val convertsString: BQRead[String] =
    new BQRead[String] {
      override val bqType: BQType =
        BQType(Field.Mode.REQUIRED, StandardSQLTypeName.STRING, Nil)

      override def read(transportSchema: avro.Schema, value: Any): String =
        value match {
          case str: String => str
          case utf: Utf8   => utf.toString
          case other =>
            sys.error(
              s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema"
            )
        }
    }

  implicit val convertsJson: BQRead[Json] =
    convertsString.map(str =>
      io.circe.parser.decode[Json](str).fold(throw _, json => json)
    )

  implicit val convertsLong: BQRead[Long] =
    new BQRead[Long] {
      override val bqType: BQType =
        BQType(Field.Mode.REQUIRED, StandardSQLTypeName.INT64, Nil)

      override def read(transportSchema: avro.Schema, value: Any): Long =
        value match {
          case long: java.lang.Long => long
          case other =>
            sys.error(
              s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema"
            )
        }
    }

  implicit val convertsDouble: BQRead[Double] =
    new BQRead[Double] {
      override val bqType: BQType =
        BQType(Field.Mode.REQUIRED, StandardSQLTypeName.FLOAT64, Nil)

      override def read(transportSchema: avro.Schema, value: Any): Double =
        value match {
          case double: java.lang.Double => double
          case other =>
            sys.error(
              s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema"
            )
        }
    }

  implicit val convertsBoolean: BQRead[Boolean] =
    new BQRead[Boolean] {
      override val bqType: BQType =
        BQType(Field.Mode.REQUIRED, StandardSQLTypeName.BOOL, Nil)

      override def read(transportSchema: avro.Schema, value: Any): Boolean =
        value match {
          case x: java.lang.Boolean => x
          case other =>
            sys.error(
              s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema"
            )
        }
    }

  implicit val convertsLocalDate: BQRead[LocalDate] =
    new BQRead[LocalDate] {
      override val bqType: BQType =
        BQType(Field.Mode.REQUIRED, StandardSQLTypeName.DATE, Nil)

      override def read(transportSchema: avro.Schema, value: Any): LocalDate =
        value match {
          case int: java.lang.Integer => LocalDate.ofEpochDay(int.toLong)
          case other =>
            sys.error(
              s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema"
            )
        }
    }

  implicit val convertsLocalTime: BQRead[LocalTime] =
    new BQRead[LocalTime] {
      override val bqType: BQType =
        BQType(Field.Mode.REQUIRED, StandardSQLTypeName.TIME, Nil)

      override def read(transportSchema: avro.Schema, value: Any): LocalTime =
        value match {
          case long: java.lang.Long => LocalTime.ofNanoOfDay(long * 1000)
          case other =>
            sys.error(
              s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema"
            )
        }
    }

  implicit val convertsInstant: BQRead[Instant] =
    new BQRead[Instant] {
      final val microsInSec = 1000 * 1000

      def fromMicros(micros: Long): Instant = {
        val secondPart = micros / microsInSec
        val microsPart = micros % microsInSec
        val nanosPart = microsPart * 1000
        Instant.ofEpochSecond(secondPart, nanosPart)
      }

      override val bqType: BQType =
        BQType(Field.Mode.REQUIRED, StandardSQLTypeName.TIMESTAMP, Nil)

      override def read(transportSchema: avro.Schema, value: Any): Instant =
        value match {
          case micros: Long => fromMicros(micros)
          case other =>
            sys.error(
              s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema"
            )
        }
    }

  implicit val convertsOffsetDateTime: BQRead[OffsetDateTime] =
    convertsInstant.map(_.atZone(ZoneOffset.UTC).toOffsetDateTime)

  implicit val convertsYearMonth: BQRead[YearMonth] =
    convertsLocalDate.map(date => YearMonth.from(date))

  implicit val convertsStringValue: BQRead[StringValue] =
    convertsString.map(StringValue.apply)
}
