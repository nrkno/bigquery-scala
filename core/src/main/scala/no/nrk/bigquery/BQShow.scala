package no.nrk.bigquery

import no.nrk.bigquery.syntax._
import com.google.cloud.bigquery.{Field, TimePartitioning}

import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalTime, YearMonth}

/** A type class, directly derived from cats.Show, which determines what a type will look like when printed to BigQuery
  * SQL
  */
@FunctionalInterface
trait BQShow[T] { self =>
  def bqShow(t: T): BQSqlFrag
  def contramap[B](f: B => T): BQShow[B] = (t: B) => self.bqShow(f(t))
}

object BQShow extends BQShowInstances {
  def apply[A](implicit instance: BQShow[A]): BQShow[A] = instance

  /** The intention, at least at first, is to be explicit what we mean when we interpolate in a string.
    *
    * If you want your string value to not be quoted:
    *   - call `BQSqlFrag(...)` manually
    *   - if the value is meant to be a column name, wrap it in [[Ident]]
    *
    * If you want your string value to be quoted then do either:
    *   - (preferably) wrap it in it's own data type with a `BQShow` instance,
    *   - wrap it in [[StringValue]]
    */
  def quoted(x: String): BQSqlFrag =
    BQSqlFrag(s"'$x'")

  /** This is the main entry point for writing BigQuery SQL statements. They are prefixed `bq` to coexist with doobie.
    *
    * Note that you can configure intellij to inject SQL language support for these: File | Settings | Languages &
    * Frameworks | Scala | Misc
    */
  class BQShowInterpolator(private val sc: StringContext) extends AnyVal {
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

}

trait BQShowInstances {

  implicit val bqShowStringValue: BQShow[StringValue] =
    x => BQShow.quoted(x.value)

  // instances below, feel free to add more
  implicit def bqShowsBQPartitionId[Pid <: BQPartitionId[Any]]: BQShow[Pid] =
    BQSqlFrag.PartitionRef.apply

  implicit def bqShowsBQPartitionIds[
      I[t] <: Iterable[t],
      Pid <: BQPartitionId[Any]
  ]: BQShow[I[Pid]] =
    partitions => BQSqlFrag.Combined(partitions.map(bqShowsBQPartitionId[Pid].bqShow).toSeq)

  implicit def bqShowTableLike[T <: BQTableLike[Unit]]: BQShow[T] =
    x => x.assertPartition.bqShow

  implicit def bqShowTablesLike[I[t] <: Iterable[t], T <: BQTableLike[Unit]]: BQShow[I[T]] =
    tables => BQSqlFrag.Combined(tables.map(_.assertPartition.bqShow).toSeq)

  implicit def bqShowWholeTable[P]: BQShow[WholeTable[P]] = x => BQSqlFrag.TableRef(x.table)

  implicit def bqShowFill[Fill <: BQFill[Any]]: BQShow[Fill] =
    BQSqlFrag.FillRef.apply

  implicit def bqShowBQFilledTable[Fill <: BQFilledTable[Any]]: BQShow[Fill] =
    BQSqlFrag.FilledTableRef.apply

  implicit def bqShowFills[
      I[t] <: Iterable[t],
      Fill <: BQFill[Any]
  ]: BQShow[I[Fill]] =
    fills => BQSqlFrag.Combined(fills.map(bqShowFill.bqShow).toSeq)

  implicit def bqShowBQLimit[T <: BQLimit]: BQShow[T] = {
    case BQLimit.Limit(value) => BQSqlFrag(s"LIMIT $value")
    case _ => BQSqlFrag.Empty
  }

  implicit val bqshowsBQType: BQShow[BQType] =
    x => BQSqlFrag(BQType.format(x))

  implicit val bqShowInt: BQShow[Int] =
    x => BQSqlFrag(x.toString)

  implicit val bqShowLong: BQShow[Long] =
    x => BQSqlFrag(x.toString)

  implicit val bqShowDouble: BQShow[Double] =
    x => BQSqlFrag(x.toString)

  implicit val bqShowBoolean: BQShow[Boolean] =
    x => BQSqlFrag(x.toString)

  implicit val bqShowLocalDate: BQShow[LocalDate] =
    x => BQSqlFrag(s"DATE('$x')")

  implicit val bqShowLocalTime: BQShow[LocalTime] =
    x => {
      // found no other way to serialize this without losing precision
      val base = BQSqlFrag(
        s"TIME(${x.getHour}, ${x.getMinute}, ${x.getSecond})"
      )
      if (x.getNano == 0) base
      else bqfr"TIME_ADD($base, INTERVAL ${x.getNano / 1000} MICROSECOND)"
    }

  implicit val bqShowInstant: BQShow[Instant] =
    x => BQSqlFrag(s"TIMESTAMP('${x.truncatedTo(ChronoUnit.MICROS)}')")

  implicit val bqShowField: BQShow[Field] =
    x => BQSqlFrag(x.getName)

  implicit val bqShowTimePartitioning: BQShow[TimePartitioning] =
    x => BQSqlFrag(x.getField)

  implicit def bqShowBQSqlFrag[F <: BQSqlFrag]: BQShow[F] =
    x => x

  implicit val bqShowYearMonth: BQShow[YearMonth] =
    x => x.atDay(1).bqShow

  implicit def option[T: BQShow]: BQShow[Option[T]] = {
    case None => BQSqlFrag("null")
    case Some(t) => t.bqShow
  }
}
