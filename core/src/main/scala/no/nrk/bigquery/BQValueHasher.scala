/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import no.nrk.bigquery.syntax.bqShowInterpolator
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets

@FunctionalInterface
trait BQValueHasher[T] {
  final def hashValueInBQ(value: Ident, range: BQIntegerRange): BQSqlFrag =
    bqsql"""MOD(CAST(CONCAT("0x", SUBSTRING(TO_HEX(SHA256($value)), 0, 14)) as INT64), ${range.end})"""
  def hashValue(value: T, range: BQIntegerRange): Long

  def contramap[Q](f: Q => T): BQValueHasher[Q] = (value: Q, range: BQIntegerRange) => hashValue(f(value), range)
}

object BQValueHasher {
  implicit def stringHasher: BQValueHasher[String] = (value: String, range: BQIntegerRange) => {
    val hash = ByteVector.apply(value.getBytes(StandardCharsets.UTF_8)).sha256.toHex
    BigInt.apply(hash.substring(0, 14), 16).longValue % range.end
  }
}
