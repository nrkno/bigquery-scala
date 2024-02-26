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
  extension (inline ctx: StringContext) {
    inline def ident(args: Any*): Ident =
      ${ IdentLiteral('ctx, 'args) }
    inline def labelkey(args: Any*): Labels.Key =
      ${ LabelKeyLiteral('ctx, 'args) }
    inline def labelvalue(args: Any*): Labels.Value =
      ${ LabelValueLiteral('ctx, 'args) }
  }
}

object IdentLiteral extends Literally[Ident] {
  def validate(s: String)(using Quotes) =
    Ident.fromString(s).map(_ => '{ Ident.unsafeFromString(${ Expr(s) }) })
}

object LabelKeyLiteral extends Literally[Labels.Key] {
  def validate(s: String)(using Quotes) =
    Labels.Key(s).toEither.left.map(_.toList.mkString("\n")).map(_ => '{ Labels.Key.make(${ Expr(s) }) })
}

object LabelValueLiteral extends Literally[Labels.Value] {
  def validate(s: String)(using Quotes) =
    Labels.Value(s).toEither.left.map(_.toList.mkString("\n")).map(_ => '{ Labels.Value.make(${ Expr(s) }) })
}
