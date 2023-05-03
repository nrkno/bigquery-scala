package no.nrk.bigquery.internal

import no.nrk.bigquery.{BQShow, BQSqlFrag}

trait BQShowSyntax {

  implicit def bqShowInterpolator(sc: StringContext): BQShow.BQShowInterpolator = new BQShow.BQShowInterpolator(sc)
  implicit def bqShowOps[A](a: A): BQShowOps[A] = new BQShowOps[A](a)
  implicit def bqFragmentsOps[S[a] <: Iterable[a], A](values: S[A]): FragmentsOps[S, A] = new FragmentsOps(values)

}

class BQShowOps[A](a: A) {

  /** Explicitly render a value with a [[BQShow]] instance to a [[BQSqlFrag]] */
  implicit def bqShow(implicit show: BQShow[A]): BQSqlFrag = BQShow[A].bqShow(a)
}

/** A way to flatten a list of fragments. The `S` just means it works for any collection data structure
  */
class FragmentsOps[S[a] <: Iterable[a], A](private val values: S[A]) extends AnyVal {
  def mkFragment(sep: String)(implicit T: BQShow[A]): BQSqlFrag =
    mkFragment(BQSqlFrag(sep))

  def mkFragment(start: String, sep: String, end: String)(implicit
      T: BQShow[A]
  ): BQSqlFrag =
    mkFragment(BQSqlFrag(start), BQSqlFrag(sep), BQSqlFrag(end))

  def mkFragment(sep: BQSqlFrag)(implicit T: BQShow[A]): BQSqlFrag =
    mkFragment(BQSqlFrag.Empty, sep, BQSqlFrag.Empty)

  def mkFragment(start: BQSqlFrag, sep: BQSqlFrag, end: BQSqlFrag)(implicit
      T: BQShow[A]
  ): BQSqlFrag = {
    val buf = Seq.newBuilder[BQSqlFrag]
    buf += start

    var first = true
    values.foreach { t =>
      if (!first) buf += sep
      first = false
      buf += BQShow[A].bqShow(t)
    }

    buf += end

    BQSqlFrag.Combined(buf.result())
  }
}
