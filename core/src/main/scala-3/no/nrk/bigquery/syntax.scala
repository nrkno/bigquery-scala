package no.nrk.bigquery

import org.typelevel.literally.Literally

object syntax {

  extension (inline ctx: StringContext)
    inline def ident(inline args: Any*): Ident =
      ${ IdentLiteral('ctx, 'args) }

  object IdentLiteral extends Literally[Ident]:
    def validate(s: String)(using Quotes) =
      Ident.fromString(s).map(_ => '{ Ident.unsafeFromString(${ Expr(s) }) })

}
