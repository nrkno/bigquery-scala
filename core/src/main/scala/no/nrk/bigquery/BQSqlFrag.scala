package no.nrk.bigquery

import no.nrk.bigquery.implicits._
import io.circe.{Encoder, Json}
import no.nrk.bigquery.BQSqlFrag.asSubQuery

/* The result of building a BigQuery sql. The `Frag` part of the name was chosen because it can be a fragment and not a complete query */
sealed trait BQSqlFrag {
  final def stripMargin: BQSqlFrag =
    this match {
      case BQSqlFrag.Frag(string) => BQSqlFrag.Frag(string.stripMargin)
      case BQSqlFrag.Combined(values) =>
        BQSqlFrag.Combined(values.map(_.stripMargin))
      case BQSqlFrag.Call(udf, args) =>
        BQSqlFrag.Call(udf, args.map(_.stripMargin))
      case x @ BQSqlFrag.PartitionRef(_)   => x
      case x @ BQSqlFrag.FillRef(_)        => x
      case x @ BQSqlFrag.FilledTableRef(_) => x
    }

  final def ++(other: BQSqlFrag): BQSqlFrag =
    BQSqlFrag.Combined(List(this, other))

  override def equals(obj: Any): Boolean =
    obj match {
      case other: BQSqlFrag => asString == other.asString
      case _                => false
    }

  override def hashCode(): Int =
    asString.hashCode

  final lazy val asString: String =
    this match {
      case BQSqlFrag.Frag(string) =>
        string
      case BQSqlFrag.Call(udf, args) =>
        args.mkFragment(bqfr"${udf.name}(", bqfr", ", bqfr")").asString

      case BQSqlFrag.Combined(values) =>
        val partitions = values.collect {
          case BQSqlFrag.FillRef(fill)     => fill.destination
          case BQSqlFrag.PartitionRef(pid) => pid
        }
        if (partitions.length == values.length) asSubQuery(partitions).asString
        else values.map(_.asString).mkString

      case BQSqlFrag.FillRef(fill) =>
        BQSqlFrag.PartitionRef(fill.destination).asString

      case BQSqlFrag.FilledTableRef(fill) =>
        BQSqlFrag
          .PartitionRef(fill.tableDef.unpartitioned.assertPartition)
          .asString

      case BQSqlFrag.PartitionRef(partitionId) =>
        partitionId match {
          case x @ BQPartitionId.MonthPartitioned(_, _) => x.asSubQuery.asString
          case x @ BQPartitionId.DatePartitioned(_, _)  => x.asSubQuery.asString
          case x @ BQPartitionId.Sharded(_, _) =>
            x.asTableId.asFragment.asString
          case x @ BQPartitionId.NotPartitioned(_) =>
            x.asTableId.asFragment.asString
        }
    }

  final lazy val asStringWithUDFs: String =
    allReferencedUDFs.map(_.definition.asString).mkString("\n\n") + asString

  final def allReferencedAsPartitions: Seq[BQPartitionId[Any]] =
    this match {
      case BQSqlFrag.Frag(_) => Nil
      case BQSqlFrag.Call(udf, args) =>
        (udf.body.allReferencedAsPartitions ++ args.flatMap(
          _.allReferencedAsPartitions
        )).distinct
      case BQSqlFrag.Combined(values) =>
        values.flatMap(_.allReferencedAsPartitions).distinct
      case BQSqlFrag.PartitionRef(ref) => List(ref)
      case BQSqlFrag.FillRef(fill)     => List(fill.destination)
      case BQSqlFrag.FilledTableRef(fill) =>
        List(fill.tableDef.unpartitioned.assertPartition)
    }

  // this does not descend into referenced fills.
  final def allReferencedUDFs: Seq[UDF] =
    this match {
      case BQSqlFrag.Frag(_) => Nil
      case BQSqlFrag.Call(udf, args) =>
        (udf.body.allReferencedUDFs ++ args.flatMap(
          _.allReferencedUDFs
        ) ++ List(udf)).distinct
      case BQSqlFrag.Combined(values) =>
        values.flatMap(_.allReferencedUDFs).distinct
      case BQSqlFrag.PartitionRef(_)   => Nil
      case BQSqlFrag.FillRef(_)        => Nil
      case BQSqlFrag.FilledTableRef(_) => Nil
    }

  override def toString: String = asString
}

object BQSqlFrag {
  def apply(string: String): BQSqlFrag = Frag(string)
  def backticks(string: String): BQSqlFrag = Frag("`" + string + "`")

  case class Frag(string: String) extends BQSqlFrag
  case class Call(udf: UDF, args: Seq[BQSqlFrag]) extends BQSqlFrag {
    require(
      udf.params.length == args.length,
      s"UDF ${udf.name.value}: Expected ${udf.params.length} arguments, got ${args.length}"
    )
  }
  case class Combined(values: Seq[BQSqlFrag]) extends BQSqlFrag
  case class PartitionRef(ref: BQPartitionId[Any]) extends BQSqlFrag
  case class FillRef(fill: BQFill) extends BQSqlFrag
  case class FilledTableRef(filledTable: BQFilledTable) extends BQSqlFrag

  val Empty: BQSqlFrag = Frag("")

  /*
   * Where `BQSqlFrag` is wanted as a parameter, we can instead ask for a `BQSqlFrag.Magnet`,
   * to enable implicit conversions to trigger. This means the called can provide values of any
   * type, as long as it is convertible to `BQSqlFrag` through a `BQShow` */
  case class Magnet(frag: BQSqlFrag) extends AnyVal
  object Magnet extends MagnetLower {
    implicit def hasInstance[T: BQShow](x: T): Magnet = Magnet(
      BQShow[T].bqShow(x)
    )
  }

  trait MagnetLower {
    // if there isn't an instance of `BQShow` available, the machinery here will provide an error message.
    // it works by defining to conflicting implicit derivation rules, which will force the compiler to stop and complain.
    // the `implicitAmbiguous` annotation provides a nicer compile error
    @scala.annotation.implicitAmbiguous("missing `BQShow[${T}]` instance")
    implicit def hasNotInstance1[T](t: T): Magnet = ???
    implicit def hasNotInstance2[T](t: T): Magnet = ???
  }

  implicit val encodes: Encoder[BQSqlFrag] = frag =>
    Json.fromString(frag.asString)

  /** Encapsulate the horribleness that is specifying more than one partition in
    * an efficient manner.
    *
    * Note that the implementation will give you what you ask for, unless the
    * list is empty
    * @throws java.lang.IllegalArgumentException
    *   if the list is empty
    */
  def asSubQuery[I[t] <: Iterable[t], Pid <: BQPartitionId[Any]](
      partitions: I[Pid]
  ): BQSqlFrag = {
    require(partitions.nonEmpty, "Cannot generate query for no partitions")

    val subSelects: List[BQSqlFrag] =
      groupByOrdered(partitions.toList.distinct)(_.wholeTable).flatMap {
        case (_, partitions) =>
          val fromSharded: Iterable[BQSqlFrag] =
            partitions.collect { case x: BQPartitionId.Sharded =>
              x
            }.sorted match {
              case sharded @ (first :: _) =>
                /* Note: it's physically impossible to perform this query
                 * without splitting the date:
                 * shard.partition.format(localDateNoDash) == for instance
                 * "20210101"
                 *
                 * If we ask for `gasessions_*` BQ will match a view and bail
                 * out So we ask for `gasessions_2*` and strip the `2` in the
                 * table suffix
                 */
                sharded
                  .map(_.partition.format(BQPartitionId.localDateNoDash))
                  .groupBy(_.head)
                  .map { case (firstDigit, formattedInSameMillenium) =>
                    val in = formattedInSameMillenium
                      .map(s => StringValue(s.drop(1)))
                      .mkFragment("[", ", ", "]")

                    val wildcard = first.wholeTable.tableId.modifyTableName(
                      _ + "_" + firstDigit.toString + "*"
                    )

                    bqfr"(select * from ${wildcard.asFragment} WHERE _TABLE_SUFFIX IN UNNEST($in))"

                  }
              case Nil => None
            }

          val fromNotPartitioned: Option[BQSqlFrag] =
            partitions.collect { case x: BQPartitionId.NotPartitioned =>
              x
            }.sorted match {
              case notPartitioned :: _ => Some(notPartitioned.asSubQuery)
              case Nil                 => None
            }

          val fromDatePartitioned: Option[BQSqlFrag] =
            partitions.collect { case x: BQPartitionId.DatePartitioned =>
              x
            }.sorted match {
              case partitions @ (first :: _) =>
                val in = partitions.map(_.partition).mkFragment("[", ", ", "]")
                Some(
                  bqfr"(select * from ${first.wholeTable.tableId.asFragment} WHERE ${first.field} IN UNNEST($in))"
                )
              case Nil => None
            }

          val fromMonthPartitioned: Option[BQSqlFrag] =
            partitions.collect { case x: BQPartitionId.MonthPartitioned =>
              x
            }.sorted match {
              case partitions @ (first :: _) =>
                val in = partitions.map(_.partition).mkFragment("[", ", ", "]")
                Some(
                  bqfr"(select * from ${first.wholeTable.tableId.asFragment} WHERE ${first.field} IN UNNEST($in))"
                )
              case Nil => None
            }

          List(
            fromSharded.toList,
            fromNotPartitioned.toList,
            fromDatePartitioned.toList,
            fromMonthPartitioned.toList
          ).flatten
      }

    subSelects match {
      case List(one) => one
      case more      => more.mkFragment("(", " UNION ALL ", ")")
    }
  }

  def groupByOrdered[T, K](ts: Seq[T])(f: T => K): List[(K, List[T])] = {
    import scala.collection.mutable

    val m = mutable.LinkedHashMap.empty[K, mutable.Builder[T, List[T]]]
    ts.foreach { t =>
      val k = f(t)
      val buf = m.getOrElse(k, List.newBuilder[T])
      m(k) = buf += t
    }

    m.result().map { case (k, ts) => (k, ts.result()) }.toList
  }
}
