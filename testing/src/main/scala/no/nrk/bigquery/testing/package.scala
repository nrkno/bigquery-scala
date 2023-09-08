/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package no.nrk.bigquery

import cats.syntax.show._
import io.circe.{Decoder, Encoder}
import no.nrk.bigquery

import scala.annotation.nowarn

package object testing {
  def tempTable(partitionId: BQPartitionId[Any]): Ident =
    Ident(partitionId.show.filter(c => c.isLetterOrDigit || c == '_'))

  implicit val encoderIdent: Encoder[Ident] = Encoder.encodeString.contramap(_.value)

  implicit lazy val encodeField: Encoder[BQField] = {
    @nowarn implicit val encodesStandardSQLTypeName: Encoder[BQField.Type] =
      Encoder[String].contramap(_.name)

    @nowarn implicit val encodesFieldMode: Encoder[bigquery.BQField.Mode] =
      Encoder[String].contramap(_.name)
    io.circe.generic.semiauto.deriveEncoder
  }

  implicit lazy val decodeField: Decoder[BQField] = {
    @nowarn implicit val decodesStandardSQLTypeName: Decoder[BQField.Type] =
      Decoder[String].map(BQField.Type.unsafeFromString)
    @nowarn implicit val decodesFieldMode: Decoder[BQField.Mode] =
      Decoder[String].map(BQField.Mode.unsafeFromString)
    io.circe.generic.semiauto.deriveDecoder
  }

  implicit val decodeSchema: Decoder[BQSchema] = Decoder.forProduct1("fields")(BQSchema.apply)
  implicit val encodeSchema: Encoder[BQSchema] = Encoder.forProduct1("fields")(_.fields)
}
