/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.data.OptionT
import cats.effect.{IO, Resource}
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import no.nrk.bigquery.client.google.GoogleBigQueryClient
import no.nrk.bigquery.metrics.MetricsOps
import org.typelevel.log4cats.slf4j.Slf4jFactory

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

object GoogleTestClient {
  private implicit val loggerFactory: Slf4jFactory[IO] = Slf4jFactory.create[IO]

  private def credentialsFromString(
      str: String
  ): IO[ServiceAccountCredentials] =
    IO.blocking(
      ServiceAccountCredentials.fromStream(
        new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8))
      )
    )

  def testClient: Resource[IO, QueryClient[IO]] =
    for {
      credentials <- Resource.eval(
        OptionT(IO(sys.env.get("BIGQUERY_SERVICE_ACCOUNT")))
          .semiflatMap(credentialsFromString)
          .getOrElseF(
            IO.blocking(GoogleCredentials.getApplicationDefault)
          )
      )
      underlying <- GoogleBigQueryClient.resource(credentials, MetricsOps.noop[IO])
    } yield underlying

}
