/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

package object util {
  type Nat = shapeless.Nat
  val nat = shapeless.nat
  type Sized[+Repr, L <: shapeless.Nat] = shapeless.Sized[Repr, L]
  val Sized = shapeless.Sized
  type SizedBuilder[CC[_]] = shapeless.SizedBuilder[CC]
}
