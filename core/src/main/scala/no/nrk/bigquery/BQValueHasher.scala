package no.nrk.bigquery

import no.nrk.bigquery.syntax.bqShowInterpolator
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets

trait BQValueHasher[T] {
  def hashValueInBQ(value: Ident, range: BQRange): BQSqlFrag =
    bqsql"""MOD(CAST(SUBSTRING(CONCAT("0x", TO_HEX(SHA256($value))), 0, 16) as INT64), ${range.end})"""
  def hashValue(value: T, range: BQRange): Long
}

object BQValueHasher {
  implicit def stringHasher: BQValueHasher[String] = (value: String, range: BQRange) => {
    val hash = ByteVector.apply(value.getBytes(StandardCharsets.UTF_8)).sha256.toHex
    BigInt.apply(hash.substring(0, 14), 16).longValue % range.end
  }
}
