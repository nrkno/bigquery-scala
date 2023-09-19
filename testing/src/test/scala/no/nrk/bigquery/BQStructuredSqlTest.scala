/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import munit.{FunSuite, Location}
import no.nrk.bigquery.syntax._
import no.nrk.bigquery.testing.BQStructuredSql.{Segment, SegmentList}
import no.nrk.bigquery.testing.{BQStructuredSql, CTE, CTEList}

class BQStructuredSqlTest extends FunSuite {
  def checkSegment(input: String, segments: Segment*)(implicit
      loc: Location
  ): Unit = {
    val actual = SegmentList(segments.toList)
    assertEquals(SegmentList.from(input), actual)
    assertEquals(input, actual.asString)
  }

  test("understand line comments") {
    checkSegment("--foo", Segment.LineComment("--foo"))
    checkSegment(" --foo", Segment.Normal(" "), Segment.LineComment("--foo"))
    checkSegment("a--foo", Segment.Normal("a"), Segment.LineComment("--foo"))
    checkSegment("#foo", Segment.LineComment("#foo"))
    checkSegment("#foo\na", Segment.LineComment("#foo\n"), Segment.Normal("a"))
  }

  test("understand block comments") {
    checkSegment(
      "a/*a */b",
      Segment.Normal("a"),
      Segment.BlockComment("/*a */"),
      Segment.Normal("b")
    )
  }

  test("understand strings") {
    checkSegment("'foo'", Segment.StrSingle("'foo'"))
    checkSegment("\"foo\"", Segment.StrDouble("\"foo\""))
    checkSegment("\"'#foo'\"", Segment.StrDouble("\"'#foo'\""))
  }

  test("not parse things within strings") {
    checkSegment("'#foo'", Segment.StrSingle("'#foo'"))
    checkSegment("'--foo'", Segment.StrSingle("'--foo'"))
    checkSegment("'\"foo\"'", Segment.StrSingle("'\"foo\"'"))
    checkSegment("'(a)'", Segment.StrSingle("'(a)'"))
  }

  test("lets escapes through") {
    checkSegment("'fla\\\'ff'", Segment.StrSingle("'fla\\\'ff'"))
    checkSegment("'fla\\\nff'", Segment.StrSingle("'fla\\\nff'"))
  }

  test("parenthesis work") {
    checkSegment(
      "asd(b(c, d))e",
      Segment.Normal("asd"),
      Segment.InParenthesis(
        List(
          Segment.Normal("b"),
          Segment.InParenthesis(List(Segment.Normal("c, d")))
        )
      ),
      Segment.Normal("e")
    )
    checkSegment(
      "a(--b\n)c",
      Segment.Normal("a"),
      Segment.InParenthesis(List(Segment.LineComment("--b\n"))),
      Segment.Normal("c")
    )
  }

  test("unterminated parenthesis") {
    checkSegment("a(a(a", Segment.Normal("a(a(a"))
    checkSegment("a(a(a()", Segment.Normal("a(a(a"), Segment.InParenthesis(Nil))
    checkSegment(
      "a(a(a('b')c",
      Segment.Normal("a(a(a"),
      Segment.InParenthesis(List(Segment.StrSingle("'b'"))),
      Segment.Normal("c")
    )
    checkSegment("(", Segment.Normal("("))
    checkSegment(")", Segment.Normal(")"))
    checkSegment("))", Segment.Normal("))"))
  }

  test("none") {
    val actual = BQStructuredSql.parse(bqfr"select 1")
    val expected = BQStructuredSql(Nil, CTEList(Nil, recursive = false), BQSqlFrag("select 1"), "select")
    assertEquals(actual, expected)
  }
  test("one") {
    val actual =
      BQStructuredSql.parse(bqfr"with a as (select ')') select * from a")
    val expected = BQStructuredSql(
      Nil,
      CTEList(List(CTE(ident"a", BQSqlFrag("(select ')')"))), recursive = false),
      BQSqlFrag(" select * from a"),
      "select"
    )
    assertEquals(actual, expected)
  }

  test("two") {
    val actual = BQStructuredSql.parse(
      bqfr"with a as (select 1), b as (select 2) select * from a, b"
    )
    val expected =
      BQStructuredSql(
        Nil,
        CTEList(
          List(
            CTE(ident"a", BQSqlFrag("(select 1)")),
            CTE(ident"b", BQSqlFrag("(select 2)"))
          ),
          recursive = false),
        BQSqlFrag(" select * from a, b"),
        "select"
      )
    assertEquals(actual, expected)
  }

  test("two with recursive") {
    val actual = BQStructuredSql.parse(
      bqfr"with recursive a as (select 1), b as (select 2) select * from a, b"
    )
    val expected =
      BQStructuredSql(
        Nil,
        CTEList(
          List(
            CTE(ident"a", BQSqlFrag("(select 1)")),
            CTE(ident"b", BQSqlFrag("(select 2)"))
          ),
          recursive = true),
        BQSqlFrag(" select * from a, b"),
        "select"
      )
    assertEquals(actual, expected)
  }

  test("comments") {
    val actual = BQStructuredSql.parse(
      bqfr"/* foo*/ /*--foo*/with/*foo*/a--foo\nas (--foo\nselect 1), b as (select 2)/*foo*/ select * from a, b--foo"
    )
    val expected = BQStructuredSql(
      Nil,
      CTEList(
        List(
          CTE(ident"a", BQSqlFrag("(--foo\nselect 1)")),
          CTE(ident"b", BQSqlFrag("(select 2)"))
        ),
        recursive = false),
      BQSqlFrag("/*foo*/ select * from a, b--foo"),
      "select"
    )
    assertEquals(actual, expected)
  }
}
