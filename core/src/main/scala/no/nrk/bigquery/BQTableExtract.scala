/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import scala.annotation.implicitNotFound
import scala.concurrent.duration.FiniteDuration

final class BQTableExtract private (
    val source: BQTableId,
    val urls: List[BQTableExtract.GSUrl],
    val format: BQTableExtract.Format,
    val compression: BQTableExtract.Compression,
    val timeout: Option[FiniteDuration]
) {
  override def toString: String =
    List(
      ("source", source.asFragment.asString),
      ("urls", urls.map(_.value).mkString("[", ", ", "]")),
      ("format", format.toString),
      ("compression", compression.value),
      ("timeout", timeout.toString)
    ).map { case (k, v) => s"$k=${v}" }.mkString("BQTableExtract(", ",\n", ")")
}

object BQTableExtract {
  def apply(source: BQTableId, urls: List[GSUrl]): BQTableExtract =
    apply(source, urls, Format.NEWLINE_DELIMITED_JSON, Compression.GZIP, None)
  def apply[F <: Format, C <: Compression](
      source: BQTableId,
      urls: List[GSUrl],
      format: F,
      compression: C,
      duration: Option[FiniteDuration])(implicit ev: FormatSupportedCompression[F, C]): BQTableExtract = {
    val _ = ev
    new BQTableExtract(source, urls, format, compression, duration)
  }

  final case class GSUrl(value: String)

  sealed abstract class Compression(val value: String)
  object Compression {
    case object NONE extends Compression("NONE")
    case object DEFLATE extends Compression("DEFLATE")
    case object GZIP extends Compression("GZIP")
    case object SNAPPY extends Compression("SNAPPY")
    case object ZSTD extends Compression("ZSTD")
  }

  sealed abstract class Format(val value: String)
  object Format {
    final case class CSV(delimiter: String = ",", printHeader: Boolean = true) extends Format("CSV")
    case object NEWLINE_DELIMITED_JSON extends Format("NEWLINE_DELIMITED_JSON")
    case object PARQUET extends Format("PARQUET")
    final case class AVRO(useLogicalTypes: Boolean = false) extends Format("AVRO")
  }

  @implicitNotFound(
    "No implicit found for ${F} and ${C}. This might mean that there is no known valid combination of format and compression")
  trait FormatSupportedCompression[F <: Format, C <: Compression]

  object FormatSupportedCompression {
    def instance[F <: Format, C <: Compression]: FormatSupportedCompression[F, C] =
      new FormatSupportedCompression[F, C] {}

    implicit val csvNone: FormatSupportedCompression[Format.CSV, Compression.NONE.type] =
      instance[Format.CSV, Compression.NONE.type]
    implicit val csvGzip: FormatSupportedCompression[Format.CSV, Compression.GZIP.type] =
      instance[Format.CSV, Compression.GZIP.type]
    implicit val jsonNone: FormatSupportedCompression[Format.NEWLINE_DELIMITED_JSON.type, Compression.NONE.type] =
      instance[Format.NEWLINE_DELIMITED_JSON.type, Compression.NONE.type]
    implicit val jsonGzip: FormatSupportedCompression[Format.NEWLINE_DELIMITED_JSON.type, Compression.GZIP.type] =
      instance[Format.NEWLINE_DELIMITED_JSON.type, Compression.GZIP.type]

    implicit val avroNone: FormatSupportedCompression[Format.AVRO, Compression.NONE.type] =
      instance[Format.AVRO, Compression.NONE.type]
    implicit val avroGzip: FormatSupportedCompression[Format.AVRO, Compression.GZIP.type] =
      instance[Format.AVRO, Compression.GZIP.type]
    implicit val avroSnappy: FormatSupportedCompression[Format.AVRO, Compression.SNAPPY.type] =
      instance[Format.AVRO, Compression.SNAPPY.type]
    implicit val avroDeflate: FormatSupportedCompression[Format.AVRO, Compression.DEFLATE.type] =
      instance[Format.AVRO, Compression.DEFLATE.type]

    implicit val parquetNone: FormatSupportedCompression[Format.PARQUET.type, Compression.NONE.type] =
      instance[Format.PARQUET.type, Compression.NONE.type]
    implicit val parquetGzip: FormatSupportedCompression[Format.PARQUET.type, Compression.GZIP.type] =
      instance[Format.PARQUET.type, Compression.GZIP.type]
    implicit val parquetDeflate: FormatSupportedCompression[Format.PARQUET.type, Compression.ZSTD.type] =
      instance[Format.PARQUET.type, Compression.ZSTD.type]
  }

}
