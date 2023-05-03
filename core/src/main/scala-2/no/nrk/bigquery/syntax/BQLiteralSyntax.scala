package no.nrk.bigquery.syntax

import no.nrk.bigquery.Ident
import org.typelevel.literally.Literally

trait BQLiteralSyntax {
  implicit final def bqLiteralSyntax(sc: StringContext): BQLiteralOps = new BQLiteralOps(sc)
}

class BQLiteralOps(val ctx: StringContext) extends AnyVal {
  def ident(args: Any*): Ident = macro BQLiteralOps.IdentLiteral.make
}
object BQLiteralOps {

  object IdentLiteral extends Literally[Ident] {

    def validate(c: Context)(s: String): Either[String, c.Expr[Ident]] = {
      import c.universe.Quasiquote
      Ident.fromString(s).map(_ => c.Expr(q"Ident.unsafeFromString($s)"))
    }

    def make(c: Context)(args: c.Expr[Any]*): c.Expr[Ident] = apply(c)(args: _*)
  }

}
