package no.nrk.bigquery

import munit.FunSuite
import no.nrk.bigquery.syntax._

/** Examples from: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#identifier_examples
  */
class IdentTest extends FunSuite {

  test("valid identifiers") {
    val invalid = List(
      "_5abc.dataField", // Valid. _5abc and dataField are valid identifiers.
      "`5abc`.dataField", // Valid. `5abc` and dataField are valid identifiers.
      "abc5.dataField", // Valid. abc5 and dataField are valid identifiers.
      "abc5.GROUP", // Valid. abc5 and GROUP are valid identifiers.
      "`GROUP`.dataField", // Valid.`GROUP` and dataField are valid identifiers
      "a.b.c",
      "foo",
      "`!`"
    ).map(input => Ident.fromString(input)).collect { case Left(err) => err }

    assert(invalid.isEmpty, clue(invalid))
  }

  test("invalid identifiers") {
    val valid = List(
      "",
      "5abc.dataField", // Invalid. 5abc is an invalid identifier because it is unquoted and starts with a number rather than a letter or underscore.
      "abc5!.dataField", // Invalid. abc5! is an invalid identifier because it is unquoted and contains a character that is not a letter, number, or underscore.
      "GROUP.dataField", // Invalid. GROUP is an invalid identifier because it is unquoted and is a stand-alone reserved keyword.
      "abc5!dataField",
      "5s",
      "!s",
      "!",
      "``"
    ).map(input => Ident.fromString(input)).collect { case Right(err) => err }

    assert(valid.isEmpty, clue(valid))
  }

  test("literal usage") {
    val i: Ident = ident"foo.bar"
    assertEquals(i, Ident("foo.bar"))
  }

}
