/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package no.nrk.bigquery.testing

import com.google.cloud.bigquery.UserDefinedFunction
import no.nrk.bigquery.UDF.Temporary
import no.nrk.bigquery._

import scala.collection.mutable

/** We need to insert functions and CTEs into a query which might already have them. In order to produce legal SQL we
  * don't really have any option other than to look into the provided SQL a bit
  *
  * This whole thing should preferably be replaced by something which can *actually* parse SQL
  */
case class BQStructuredSql(
    udfs: List[UserDefinedFunction],
    ctes: CTEList,
    query: BQSqlFrag,
    queryType: String
) {
  lazy val asFragment: BQSqlFrag = {
    val combinedUdfString: Option[String] =
      udfs match {
        case Nil => None
        case nonEmpty =>
          val nonEmptyStrings = nonEmpty.map {
            case f if f.getType == UserDefinedFunction.Type.INLINE =>
              require(
                f.getContent.endsWith(";"),
                s"${f.getContent} should end with `;`"
              )
              f.getContent
            case f => sys.error(s"UDF of type ${f.getType} not supported yet")
          }
          Some(nonEmptyStrings.mkString("", "\n", "\n"))
      }

    val combinedCteString: Option[String] = ctes.definition.map(_.asString)

    BQSqlFrag(
      List(combinedUdfString, combinedCteString, Some(query)).flatten
        .mkString("\n")
    )
  }
}

object BQStructuredSql {

  /** This will do its best to separate UDFs and CTEs from the rest of the query.
    */
  def parse(fullQuery: BQSqlFrag): BQStructuredSql = {
    val segmentList = SegmentList.from(fullQuery.asString)
    val split = segmentList.split(';')
    val functionSegmentLists = split.init
    val querySegmentList = split.last

    val functions: List[UserDefinedFunction] = {
      val fs1 = fullQuery.allReferencedUDFs
        .collect { case udf: Temporary => UserDefinedFunction.inline(udf.definition.asString) }
      val fs2 = functionSegmentLists.map(segmentList => UserDefinedFunction.inline(segmentList.asString + ";"))
      fs1.toList ++ fs2
    }
    val (ctes, queryPartSegments) = extractCTEs(querySegmentList)
    val queryType = queryPartSegments.segs.flatMap {
      case Segment.Normal(text) =>
        text.split("\\b").map(_.trim).filter(_.nonEmpty)
      case _ => Nil
    }.head
    BQStructuredSql(
      functions,
      ctes,
      BQSqlFrag(queryPartSegments.asString),
      queryType
    )
  }

  def extractCTEs(segments: SegmentList): (CTEList, SegmentList) = {
    val rebalanced: SegmentList = segments.words

    val relevant: Seq[(Segment, Int)] =
      rebalanced.segs.zipWithIndex.collect {
        case (x: Segment.Normal, originalIdx) if x.asString.trim.nonEmpty =>
          (x, originalIdx)
        case (x: Segment.InParenthesis, originalIdx) => (x, originalIdx)
      }

    // custom extractor
    object Keyword {
      def unapply(seg: (Segment, Int)): Some[String] = Some(
        seg._1.asString.toLowerCase.trim
      )
    }

    relevant match {
      case Keyword("with") :: rest =>
        def go(rest: List[(Segment, Int)]): (List[CTE], Int) =
          rest match {
            case (name, _) :: Keyword("as") :: (
                  p: Segment.InParenthesis,
                  endIdx
                ) :: rest =>
              val foundCte = CTE(Ident(name.asString), BQSqlFrag(p.asString))
              rest match {
                case Keyword(",") :: rest => // continue
                  val (nextCtes, endIdx) = go(rest)
                  (foundCte :: nextCtes, endIdx)
                case _ =>
                  (List(foundCte), endIdx)
              }
            case _ =>
              sys.error(
                s"expected CTE at ${rest.map(_._1.asString).mkString.take(50)}"
              )
          }
        val (recur, (ctes, endIdx)) = rest match {
          case Keyword("recursive") :: actualrest => true -> go(actualrest)

          case rest => false -> go(rest)
        }

        val query = SegmentList(rebalanced.segs.drop(endIdx + 1))

        (CTEList(ctes, recur), query)
      case _ => (CTEList(Nil, recursive = false), rebalanced)
    }
  }

  sealed trait Segment {
    def asString: String
  }

  object Segment {
    case class Normal(asString: String) extends Segment

    case class LineComment(asString: String) extends Segment

    case class BlockComment(asString: String) extends Segment

    case class StrDouble(asString: String) extends Segment {
      require(asString.startsWith("\"") && asString.endsWith("\""))
    }

    case class StrSingle(asString: String) extends Segment {
      require(asString.startsWith("'") && asString.endsWith("'"))
    }

    case class InParenthesis(values: List[Segment]) extends Segment {
      override def asString: String =
        values.map(_.asString).mkString("(", "", ")")
    }
  }

  case class SegmentList(segs: Seq[Segment]) {
    def asString: String =
      segs.map(_.asString).mkString

    def words: SegmentList = {
      def explode(segment: Segment): List[Segment] =
        segment match {
          case Segment.Normal(asString) =>
            asString.split("\\b").map(x => Segment.Normal(x)).toList
          case Segment.InParenthesis(values) =>
            List(Segment.InParenthesis(values.flatMap(explode)))
          case other => List(other)
        }

      SegmentList(segs.flatMap(explode))
    }

    // does not descend into parenthesis
    def split(sep: Char): List[SegmentList] = {
      val ret = List.newBuilder[SegmentList]
      val current = List.newBuilder[Segment]
      segs.foreach {
        case normal @ Segment.Normal(value) =>
          value.split(sep) match {
            case Array(one @ _) =>
              current += normal
            case more =>
              more.zipWithIndex.foreach { case (one, idx) =>
                current += Segment.Normal(one)
                val isLast = idx == more.length - 1
                if (!isLast) {
                  ret += SegmentList(current.result())
                  current.clear()
                }
              }
          }
        case other => current += other
      }
      ret += SegmentList(current.result())
      ret.result()
    }
  }

  object SegmentList {
    private sealed trait State

    private object State {
      case object Normal extends State

      case object LineComment extends State

      case object BlockComment extends State

      case object StrDouble extends State

      case object StrSingle extends State
    }

    def from(sql: String): SegmentList = {
      // use `segments` to describe what we have collected so far. we need the stack to account for matching parentheses
      val segmentsStack = mutable.Stack(mutable.ArrayBuffer.empty[Segment])

      def segments = segmentsStack.head

      val currentSegment = new StringBuilder()
      var idx = 0
      var lastState: State = State.Normal

      def stateChange(newState: State): Unit = {
        val segmentText = currentSegment.result()
        if (segmentText.nonEmpty) {
          val created = lastState match {
            case State.Normal => Segment.Normal(segmentText)
            case State.LineComment => Segment.LineComment(segmentText)
            case State.BlockComment => Segment.BlockComment(segmentText)
            case State.StrDouble => Segment.StrDouble(segmentText)
            case State.StrSingle => Segment.StrSingle(segmentText)
          }
          segments += created
          currentSegment.clear()
        }
        lastState = newState
      }

      while (idx < sql.length) {
        def nextIs(char: Char): Boolean =
          idx + 1 < sql.length && (sql(idx + 1) == char)

        def currentChar = sql(idx)

        def add() = currentSegment += currentChar

        (currentChar, lastState) match {
          case ('"', State.Normal) =>
            stateChange(State.StrDouble)
            add()
          case ('"', State.StrDouble) =>
            add()
            stateChange(State.Normal)
          case ('\'', State.Normal) =>
            stateChange(State.StrSingle)
            add()
          case ('\'', State.StrSingle) =>
            add()
            stateChange(State.Normal)
          case ('/', State.Normal) if nextIs('*') =>
            stateChange(State.BlockComment)
            add()
          case ('*', State.BlockComment) if nextIs('/') =>
            add()
            idx += 1
            add()
            stateChange(State.Normal)
          case ('\n', State.LineComment) =>
            add()
            stateChange(State.Normal)
          case ('-', State.Normal) if nextIs('-') =>
            stateChange(State.LineComment)
            add()
            idx += 1
            add()
          case ('#', State.Normal) =>
            stateChange(State.LineComment)
            add()
          case (
                '\\',
                _
              ) => // consume escapes, don't think we need to look into them
            add()
            idx += 1
            add()
          case ('(', State.Normal) =>
            stateChange(State.Normal) // trigger commit
            segmentsStack.push(mutable.ArrayBuffer.empty)
          case (')', State.Normal) if segmentsStack.size > 1 =>
            stateChange(State.Normal) // trigger commit
            val collected = segmentsStack.pop()
            segments += Segment.InParenthesis(collected.toList)
          case _ =>
            add()
        }

        idx += 1
      }

      // commit remaining
      stateChange(State.Normal)

      // unwind parenthesis stack in case of non-closed parentheses
      val completeSegments = segmentsStack.reverse.zipWithIndex.flatMap {
        case (segments, 0) => segments
        case (segments, _) => Segment.Normal("(") +: segments
      }

      // clean up output from parenthesis handling
      val recombinedTextSegments =
        completeSegments.foldRight(List.empty[Segment]) {
          case (Segment.Normal(_1), Segment.Normal(_2) :: tail) =>
            Segment.Normal(_1 + _2) :: tail
          case (s, acc) => s :: acc
        }

      SegmentList(recombinedTextSegments)
    }
  }
}
