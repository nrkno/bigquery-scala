/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.Show
import cats.syntax.all._

import scala.util.matching.Regex

/** an identifier in an sql statement, typically a column name or anything which shouldnt be quoted
  */
case class Ident(value: String) extends AnyVal {
  def suffixed(next: String): Ident = Ident(s"$value$next")
  def prefixed(pre: String): Ident = Ident(s"$pre$value")
}

object Ident {
  // format: off
  // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#reserved_keywords
  val keywords = Set("ALL", "AND", "ANY", "ARRAY", "AS", "ASC", "ASSERT_ROWS_MODIFIED", "AT", "BETWEEN", "BY", "CASE", "CAST", "COLLATE", "CONTAINS", "CREATE", "CROSS", "CUBE", "CURRENT", "DEFAULT", "DEFINE", "DESC", "DISTINCT", "ELSE", "END", "ENUM", "ESCAPE", "EXCEPT", "EXCLUDE", "EXISTS", "EXTRACT", "FALSE", "FETCH", "FOLLOWING", "FOR", "FROM", "FULL", "GROUP", "GROUPING", "GROUPS", "HASH", "HAVING", "IF", "IGNORE", "IN", "INNER", "INTERSECT", "INTERVAL", "INTO", "IS", "JOIN", "LATERAL", "LEFT", "LIKE", "LIMIT", "LOOKUP", "MERGE", "NATURAL", "NEW", "NO", "NOT", "NULL", "NULLS", "OF", "ON", "OR", "ORDER", "OUTER", "OVER", "PARTITION", "PRECEDING", "PROTO", "QUALIFY", "RANGE", "RECURSIVE", "RESPECT", "RIGHT", "ROLLUP", "ROWS", "SELECT", "SET", "SOME", "STRUCT", "TABLESAMPLE", "THEN", "TO", "TREAT", "TRUE", "UNBOUNDED", "UNION", "UNNEST", "USING", "WHEN", "WHERE", "WINDOW", "WITH", "WITHIN")
  // format: on

  private val validationRegex = {
    val base = "(([[\\p{Alpha}]_][[\\p{Alnum}]_]*)|(`[^`]+`))"
    new Regex(s"^$base(\\.$base)*$$")
  }

  def fromString(str: String): Either[String, Ident] =
    validationRegex
      .findAllMatchIn(str)
      .toList match {
      case validIdentPattern :: Nil =>
        val firstIdentGroup = validIdentPattern.group(1)
        if (keywords.contains(firstIdentGroup.toUpperCase()))
          Left(show"Keyword `$firstIdentGroup` not allowed in the first position")
        else Right(new Ident(str))
      case _ => Left(show"The string `$str` is not a valid unquoted identifier")
    }

  def unsafeFromString(str: String): Ident =
    fromString(str).fold(msg => throw new IllegalStateException(msg), identity)

  implicit val bqShowIdent: BQShow[Ident] =
    x =>
      if (keywords(x.value.toUpperCase) || x.value.contains("-"))
        BQSqlFrag("`" + x.value + "`")
      else BQSqlFrag(x.value)

  implicit val showIdent: Show[Ident] =
    x => x.value

}
