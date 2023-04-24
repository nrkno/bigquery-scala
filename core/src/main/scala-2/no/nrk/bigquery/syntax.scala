package no.nrk.bigquery

import org.typelevel.literally.Literally

object syntax {

  implicit class BQLiteralMacros(val sc: StringContext) extends AnyVal {
    def ident(args: Any*): Ident = macro IdentLiteral.make
  }

  object IdentLiteral extends Literally[Ident] {

    def validate(c: Context)(s: String): Either[String, c.Expr[Ident]] = {
      import c.universe.Quasiquote
      Ident.fromString(s).map(_ => c.Expr(q"Ident.unsafeFromString($s)"))
    }

    def make(c: Context)(args: c.Expr[Any]*): c.Expr[Ident] = apply(c)(args: _*)
  }

}
