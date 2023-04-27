package no.nrk.bigquery

import cats.Show
import com.google.cloud.bigquery.{Option => _, _}

object implicits {
  // suppress not used warnings
  private[bigquery] def assertIsUsed(a: Any*): Unit = (a, ())._2

  /** This is the main entry point for writing BigQuery SQL statements. They are prefixed `bq` to coexist with doobie.
    *
    * Note that you can configure intellij to inject SQL language support for these: File | Settings | Languages &
    * Frameworks | Scala | Misc
    */
  final implicit class BQShowInterpolator(private val sc: StringContext) extends AnyVal {
    def bqsql(args: BQSqlFrag.Magnet*): BQSqlFrag = {
      // intersperse args into the interpolated string in `sc.parts`
      val builder = List.newBuilder[BQSqlFrag]
      var idx = 0
      while (idx < sc.parts.length) {
        builder += BQSqlFrag.Frag(StringContext.processEscapes(sc.parts(idx)))
        if (idx < args.length) {
          builder += args(idx).frag
        }
        idx += 1
      }

      builder.result() match {
        case Nil => BQSqlFrag.Empty
        case one :: Nil => one
        case many => BQSqlFrag.Combined(many)
      }
    }

    def bqfr(args: BQSqlFrag.Magnet*): BQSqlFrag = bqsql(args: _*)
  }

  /** A way to flatten a list of fragments. The `S` just means it works for any collection data structure
    */
  implicit class MakeFragmentSyntax[S[a] <: Iterable[a], A](
      private val values: S[A]
  ) extends AnyVal {
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

  /** Explicitly render a value with a [[BQShow]] instance to a [[BQSqlFrag]]
    */
  final implicit class BQShowSyntax[A](private val a: A) extends AnyVal {
    def bqShow(implicit show: BQShow[A]): BQSqlFrag = BQShow[A].bqShow(a)
  }

  // orphan instances below
  implicit val showJobId: Show[JobId] = Show.show(Jsonify.jobId)
  implicit val showBigQueryError: Show[BigQueryError] = Show.show(Jsonify.error)
  implicit def showJob[J <: JobInfo]: Show[J] = Show.show(Jsonify.job)
}
