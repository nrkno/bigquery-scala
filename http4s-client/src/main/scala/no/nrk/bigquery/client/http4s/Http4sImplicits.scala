/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.client.http4s

import cats.effect.Concurrent
import io.circe.{Decoder, Encoder}
import org.http4s.{EntityDecoder, EntityEncoder}

private[client] object Http4sImplicits {
  private[client] implicit def entityEncoder[F[_], A: Encoder]: EntityEncoder[F, A] =
    org.http4s.circe.jsonEncoderOf[F, A]

  private[client] implicit def entityDecoder[F[_]: Concurrent, A: Decoder]: EntityDecoder[F, A] =
    org.http4s.circe.jsonOf[F, A]

}
