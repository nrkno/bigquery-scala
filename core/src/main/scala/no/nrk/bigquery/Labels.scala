/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.syntax.all.*

object Labels {
  final class Key private (val value: String) extends AnyVal {
    override def toString: String = value
  }
  object Key {
    def apply(key: String) =
      (
        check(key)(_.isEmpty, show"label key $key is empty"),
        checkLength(key, "key"),
        checkChars(key, "key"),
        check(key)(!_(0).isLower, show"label key $key must start with lowercase letter or international character"))
        .mapN((_, _, _, _) => new Key(key))

    def unsafeFromString(value: String): Key =
      apply(value).fold(messages => throw new IllegalArgumentException(messages.toList.mkString("\n")), identity)

    private[bigquery] def make(value: String) = new Key(value)
  }

  final class Value private[bigquery] (val value: String) extends AnyVal {
    override def toString: String = value
  }

  object Value {
    def apply(value: String) =
      (checkLength(value, "value"), checkChars(value, "value")).mapN((_, _) => new Value(value))

    def unsafeFromString(value: String): Value =
      apply(value).fold(messages => throw new IllegalArgumentException(messages.toList.mkString("\n")), identity)

    private[bigquery] def make(value: String) = new Value(value)
  }

  private def check(key: String)(op: String => Boolean, msg: String) =
    if (op(key)) msg.invalidNec else key.validNec

  private def checkLength(v: String, mode: String) =
    check(v)(_.length > 63, show"label $mode $v is longer than 63 chars")

  private def checkChars(v: String, mode: String) = check(v)(
    !_.forall(c => c.isLower || c.isDigit || c == '-' || c == '_'),
    show"label $mode $v can contain only lowercase letters, numeric characters, underscores, and dashes"
  )
}
