package no.nrk.bigquery

import no.nrk.bigquery.syntax.bqShowInterpolator
import org.apache.commons.codec.digest.DigestUtils

trait BQValueHasher[T] {
  val range: BQRange
  def hashValueInBQ(value: Ident): BQSqlFrag
  def hashValue(value: T): Long
}

object BQValueHasher {
  case class ShaHasher(range: BQRange) extends BQValueHasher[String] {
    val digester = new DigestUtils("SHA-256")

    override def hashValueInBQ(value: Ident): BQSqlFrag =
      bqsql"""MOD(CAST(SUBSTRING(CONCAT("0x", TO_HEX(SHA256($value))), 0, 16) as INT64), ${range.end})"""

    override def hashValue(value: String): Long = {
      val hash = digester.digestAsHex(value)
      BigInt.apply(hash.substring(0, 14), 16).longValue % range.end
    }
  }
}
