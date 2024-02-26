/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.internal

import cats.syntax.all.*
import no.nrk.bigquery.{Ident, Labels}
import org.typelevel.literally.Literally

trait BQLiteralSyntax {
  implicit final def bqLiteralSyntax(sc: StringContext): BQLiteralOps = new BQLiteralOps(sc)
}

class BQLiteralOps(val ctx: StringContext) extends AnyVal {
  def ident(args: Any*): Ident = macro BQLiteralOps.IdentLiteral.make
  def labelkey(args: Any*): Labels.Key = macro BQLiteralOps.LabelKeyLiteral.make
  def labelvalue(args: Any*): Labels.Value = macro BQLiteralOps.LabelValueLiteral.make
}
object BQLiteralOps {

  object IdentLiteral extends Literally[Ident] {

    def validate(c: Context)(s: String): Either[String, c.Expr[Ident]] = {
      import c.universe.Quasiquote
      Ident.fromString(s).map(_ => c.Expr(q"Ident.unsafeFromString($s)"))
    }

    def make(c: Context)(args: c.Expr[Any]*): c.Expr[Ident] = apply(c)(args*)
  }

  object LabelKeyLiteral extends Literally[Labels.Key] {

    def validate(c: Context)(s: String): Either[String, c.Expr[Labels.Key]] = {
      import c.universe.Quasiquote
      Labels.Key.apply(s).toEither.left.map(_.toList.mkString("\n")).map(_ => c.Expr(q"Labels.Key.make($s)"))
    }

    def make(c: Context)(args: c.Expr[Any]*): c.Expr[Labels.Key] = apply(c)(args*)
  }

  object LabelValueLiteral extends Literally[Labels.Value] {

    def validate(c: Context)(s: String): Either[String, c.Expr[Labels.Value]] = {
      import c.universe.Quasiquote
      Labels.Value.apply(s).toEither.left.map(_.toList.mkString("\n")).map(_ => c.Expr(q"Labels.Value.make($s)"))
    }

    def make(c: Context)(args: c.Expr[Any]*): c.Expr[Labels.Value] = apply(c)(args*)
  }

}
