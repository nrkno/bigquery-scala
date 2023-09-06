package no.nrk.bigquery

package object internal {
  type Nat = shapeless.Nat
  val nat = shapeless.nat
  type Sized[+Repr, L <: shapeless.Nat] = shapeless.Sized[Repr, L]
  val Sized = shapeless.Sized
  type SizedBuilder[CC[_]] = shapeless.SizedBuilder[CC]
}
