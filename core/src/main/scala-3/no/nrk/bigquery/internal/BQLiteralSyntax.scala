package no.nrk.bigquery.internal

import no.nrk.bigquery.Ident
import org.typelevel.literally.Literally

trait BQLiteralSyntax {
  extension (inline ctx: StringContext)
    inline def ident(args: Any*): Ident =
      ${ IdentLiteral('ctx, 'args) }
}

object IdentLiteral extends Literally[Ident]:
  def validate(s: String)(using Quotes) =
    Ident.fromString(s).map(_ => '{ Ident.unsafeFromString(${ Expr(s) }) })
