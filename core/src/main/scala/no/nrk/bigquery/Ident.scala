package no.nrk.bigquery

import cats.Show
import cats.data.NonEmptyList
import cats.syntax.all._
import cats.parse.Rfc5234.{alpha, digit}
import cats.parse.{Parser => P}

/** an identifier in an sql statement, typically a column name or anything which shouldn't be quoted
  */
case class Ident(segments: NonEmptyList[String]) {
  override def toString: String =
    show"Ident($this)"

  def suffixed(next: String): Ident = {
    val revSeg = segments.reverse
    Ident(NonEmptyList.of(revSeg.head ++ next, revSeg.tail: _*).reverse)
  }

  def prefixed(pre: String): Ident =
    Ident(NonEmptyList.of(pre ++ segments.head, segments.tail: _*))
}

object Ident {

  @deprecated("use string interpolation `indent\"value\"` ")
  def apply(value: String) = new Ident(NonEmptyList.one(value))

  // format: off
  // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#reserved_keywords
  val keywords = Set("ALL", "AND", "ANY", "ARRAY", "AS", "ASC", "ASSERT_ROWS_MODIFIED", "AT", "BETWEEN", "BY", "CASE", "CAST", "COLLATE", "CONTAINS", "CREATE", "CROSS", "CUBE", "CURRENT", "DEFAULT", "DEFINE", "DESC", "DISTINCT", "ELSE", "END", "ENUM", "ESCAPE", "EXCEPT", "EXCLUDE", "EXISTS", "EXTRACT", "FALSE", "FETCH", "FOLLOWING", "FOR", "FROM", "FULL", "GROUP", "GROUPING", "GROUPS", "HASH", "HAVING", "IF", "IGNORE", "IN", "INNER", "INTERSECT", "INTERVAL", "INTO", "IS", "JOIN", "LATERAL", "LEFT", "LIKE", "LIMIT", "LOOKUP", "MERGE", "NATURAL", "NEW", "NO", "NOT", "NULL", "NULLS", "OF", "ON", "OR", "ORDER", "OUTER", "OVER", "PARTITION", "PRECEDING", "PROTO", "QUALIFY", "RANGE", "RECURSIVE", "RESPECT", "RIGHT", "ROLLUP", "ROWS", "SELECT", "SET", "SOME", "STRUCT", "TABLESAMPLE", "THEN", "TO", "TREAT", "TRUE", "UNBOUNDED", "UNION", "UNNEST", "USING", "WHEN", "WHERE", "WINDOW", "WITH", "WITHIN")
  // format: on

  def fromString(str: String): Either[String, Ident] =
    IdentParser.parse(str)

  def unsafeFromString(str: String): Ident =
    fromString(str).fold(msg => throw new IllegalStateException(msg), identity)

  implicit val bqShowIdent: BQShow[Ident] =
    ident => BQSqlFrag(ident.show)

  implicit val showIdent: Show[Ident] =
    ident => {
      val parts = ident.segments
      val head = if (keywords(parts.head.toUpperCase())) "`" + parts.head + "`" else parts.head
      (head :: parts.tail).mkString(".")
    }

}

object IdentParser {
  private val underscore = P.char('_')
  private val backtick = P.char('`')
  private val period = P.char('.')
  private val quotedChar = {
    val invalidChars = "`.*(){}[]".toCharArray.toSet
    P.charsWhile(!invalidChars(_))
  }

  private val unquoted: P[String] =
    (P.oneOf(alpha :: underscore :: Nil) ~ P.oneOf(alpha :: underscore :: digit :: Nil).rep0).string
  private val quoted = quotedChar.rep.surroundedBy(backtick).string

  def parse(input: String): Either[String, Ident] =
    P
      .oneOf(unquoted :: quoted :: Nil)
      .repSep(period | P.end)
      .parseAll(input)
      .leftMap((err: P.Error) => err.show)
      .flatMap(segments =>
        if (Ident.keywords(segments.head)) Left("Fist segment can not be a keyword:")
        else Right(new Ident(segments)))

}
