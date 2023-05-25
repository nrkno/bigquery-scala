package no.nrk.bigquery.internal

import no.nrk.bigquery.{BQFill, BQFilledTable, BQShow, BQSqlFrag}
import cats.Foldable
import cats.syntax.all._

import java.time.LocalDate

trait BQShowSyntax {

  implicit def bqShowInterpolator(sc: StringContext): BQShow.BQShowInterpolator = new BQShow.BQShowInterpolator(sc)
  implicit def bqShowOps[A](a: A): BQShowOps[A] = new BQShowOps[A](a)
  implicit def bqFragmentsOps[S[_]: Foldable, A](values: S[A]): FragmentsOps[S, A] = new FragmentsOps(values)
  implicit def bqFilledTableLocalDateOps(fill: BQFilledTable[LocalDate]): BQFilledTableLocalDateOps =
    new BQFilledTableLocalDateOps(fill)

}

class BQShowOps[A](a: A) {

  /** Explicitly render a value with a [[BQShow]] instance to a [[BQSqlFrag]] */
  implicit def bqShow(implicit show: BQShow[A]): BQSqlFrag = BQShow[A].bqShow(a)
}

/** A way to flatten a list of fragments. The `S` just means it works for any collection data structure
  */
class FragmentsOps[S[_]: Foldable, A](private val values: S[A]) {
  def mkFragment(sep: String)(implicit T: BQShow[A]): BQSqlFrag =
    mkFragment(BQSqlFrag(sep))

  def mkFragment(start: String, sep: String, end: String)(implicit T: BQShow[A]): BQSqlFrag =
    mkFragment(BQSqlFrag(start), BQSqlFrag(sep), BQSqlFrag(end))

  def mkFragment(sep: BQSqlFrag)(implicit T: BQShow[A]): BQSqlFrag =
    mkFragment(BQSqlFrag.Empty, sep, BQSqlFrag.Empty)

  def mkFragment(start: BQSqlFrag, sep: BQSqlFrag, end: BQSqlFrag)(implicit T: BQShow[A]): BQSqlFrag = {
    val buf = Seq.newBuilder[BQSqlFrag]
    buf += start

    values.foldLeft(true) { (first, t) =>
      if (!first) buf += sep
      buf += BQShow[A].bqShow(t)
      false
    }

    buf += end

    BQSqlFrag.Combined(buf.result())
  }
}

class BQFilledTableLocalDateOps(fill: BQFilledTable[LocalDate]) {

  def withDate(partitionValue: LocalDate): BQFill[LocalDate] =
    BQFill[LocalDate](fill.jobKey, fill.tableDef, fill.query(partitionValue), partitionValue)
}
