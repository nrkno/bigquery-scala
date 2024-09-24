/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import io.circe.Json

import org.apache.avro.util.Utf8
import org.apache.avro

import java.time.*
import scala.collection.compat.*
import scala.jdk.CollectionConverters.*

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

object BQRead extends BQReadCompat {

  def apply[A: BQRead]: BQRead[A] = implicitly

  private[bigquery] def firstNotNullable(
      schema: avro.Schema
  ): Option[avro.Schema] =
    if (schema.isUnion) schema.getTypes.asScala.collectFirst {
      case s if !s.isNullable => s
    }
    else None

  implicit def convertsOption[A: BQRead]: BQRead[Option[A]] =
    new BQRead[Option[A]] {
      override val bqType: BQType = {
        val base = BQRead[A].bqType
        if (base.mode == BQField.Mode.REQUIRED)
          base.copy(mode = BQField.Mode.NULLABLE)
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
        BQRead[A].bqType.copy(mode = BQField.Mode.REPEATED)

      override def read(transportSchema: avro.Schema, value: Any): I[A] = {
        val b = cb.newBuilder
        value match {
          case coll: scala.Array[?] =>
            coll.foreach(elem => b += BQRead[A].read(transportSchema.getElementType, elem))
          case coll: java.util.Collection[?] =>
            coll.forEach(elem => b += BQRead[A].read(transportSchema.getElementType, elem))
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
        BQRead[A].bqType.copy(mode = BQField.Mode.REPEATED)

      override def read(transportSchema: avro.Schema, value: Any): Array[A] = {
        val b = Array.newBuilder[A]
        value match {
          case coll: scala.Array[?] =>
            coll.foreach(elem => b += BQRead[A].read(transportSchema.getElementType, elem))
          case coll: java.util.Collection[?] =>
            coll.forEach(elem => b += BQRead[A].read(transportSchema.getElementType, elem))
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
        BQType(BQField.Mode.REQUIRED, BQField.Type.STRING, Nil)

      override def read(transportSchema: avro.Schema, value: Any): String =
        value match {
          case str: String => str
          case utf: Utf8 => utf.toString
          case other =>
            sys.error(
              s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema"
            )
        }
    }

  implicit val convertsJson: BQRead[Json] =
    convertsString.map(str => io.circe.parser.decode[Json](str).fold(throw _, json => json))

  implicit val convertsLong: BQRead[Long] =
    new BQRead[Long] {
      override val bqType: BQType =
        BQType(BQField.Mode.REQUIRED, BQField.Type.INT64, Nil)

      override def read(transportSchema: avro.Schema, value: Any): Long =
        value match {
          case long: java.lang.Long => long
          case other =>
            sys.error(
              s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema"
            )
        }
    }

  implicit val convertsInt: BQRead[Int] =
    new BQRead[Int] {
      override val bqType: BQType =
        BQType(BQField.Mode.REQUIRED, BQField.Type.INT64, Nil)

      override def read(transportSchema: avro.Schema, value: Any): Int =
        value match {
          case int: java.lang.Integer => int
          case other =>
            sys.error(
              s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema"
            )
        }
    }

  implicit val convertsDouble: BQRead[Double] =
    new BQRead[Double] {
      override val bqType: BQType =
        BQType(BQField.Mode.REQUIRED, BQField.Type.FLOAT64, Nil)

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
        BQType(BQField.Mode.REQUIRED, BQField.Type.BOOL, Nil)

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
        BQType(BQField.Mode.REQUIRED, BQField.Type.DATE, Nil)

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
        BQType(BQField.Mode.REQUIRED, BQField.Type.TIME, Nil)

      override def read(transportSchema: avro.Schema, value: Any): LocalTime =
        value match {
          case long: java.lang.Long => LocalTime.ofNanoOfDay(long * 1000)
          case other =>
            sys.error(
              s"Unexpected: ${other.getClass.getSimpleName} $other . Schema from BQ: $transportSchema"
            )
        }
    }

  implicit val convertsLocalDateTime: BQRead[LocalDateTime] =
    new BQRead[LocalDateTime] {
      override val bqType: BQType =
        BQType(BQField.Mode.REQUIRED, BQField.Type.DATETIME, Nil)

      override def read(transportSchema: avro.Schema, value: Any): LocalDateTime =
        value match {
          case string: java.lang.String => LocalDateTime.parse(string)
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
        BQType(BQField.Mode.REQUIRED, BQField.Type.TIMESTAMP, Nil)

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
