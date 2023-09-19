/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import org.scalacheck.{Arbitrary, Gen}

object Generators {
  def shorterThan(n: Int, gen: Gen[String]): Gen[String] =
    gen.map(str => if (str.length < n) str else str.substring(0, n))

  def shorterThanAlphaNum(n: Int): Gen[String] = shorterThan(n, Gen.alphaNumStr)

  val unicodeIdentifierPart = Arbitrary.arbChar.arbitrary
    .filter(c => Character.isUnicodeIdentifierPart(c) && !Character.isWhitespace(c))

  val validProjectIdGen = for {
    firstchar <- Gen.stringOfN(1, Gen.alphaLowerChar)
    maybeSymbol <- Gen.oneOf("", "-")
    alphaNumLower = Gen.oneOf(Gen.alphaLowerChar, Gen.numChar)
    afterdash <- Gen.stringOfN(6 - maybeSymbol.length, alphaNumLower)
    lastChars <- Generators
      .shorterThan(29 - (1 + afterdash.length + maybeSymbol.length), Gen.stringOf(alphaNumLower))
  } yield firstchar + maybeSymbol + afterdash + lastChars

  val validDatasetIdGen = for {
    alpha <- Generators.shorterThanAlphaNum(1021).filterNot(_.isEmpty)
    underscore <- Gen.oneOf("", "_")
    alpha2 <- Generators.shorterThanAlphaNum(2).filterNot(_.isEmpty)
  } yield alpha + underscore + alpha2

  val validTableIdGen = for {
    first <- Gen
      .stringOfN(1, unicodeIdentifierPart)
      .map(_.replaceAll("(?U)\\W", ""))
      .map(s => if (s.isEmpty) "a" else s)
    choose <- Gen.oneOf("", " ", "-", "_", "$", "*")
    next <- Generators.shorterThan(
      1022,
      Gen.stringOf(unicodeIdentifierPart).map(_.replaceAll("(?U)\\W", "")).filter(_.nonEmpty))
  } yield first + choose + next
}
