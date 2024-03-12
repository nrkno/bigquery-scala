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
      clientDefaults <- Resource.eval(IO {
        val project = sys.env
          .get("BIGQUERY_DEFAULT_PROJECT")
          .map(ProjectId.unsafeFromString)
          .getOrElse(ProjectId.unsafeFromString("bigquery-public-data"))
        val location = sys.env
          .get("BIGQUERY_DEFAULT_LOCATION")
          .map(LocationId.apply)
          .getOrElse(LocationId.US)
        BQClientDefaults(project, location)
      })
      client <- testClient(clientDefaults)
    } yield client

  def testClient(defaults: BQClientDefaults): Resource[IO, QueryClient[IO]] =
    for {
      credentials <- Resource.eval(
        OptionT(IO(sys.env.get("BIGQUERY_SERVICE_ACCOUNT")))
          .semiflatMap(credentialsFromString)
          .getOrElseF(
            IO.blocking(GoogleCredentials.getApplicationDefault)
          )
      )
      underlying <- GoogleBigQueryClient.resource(
        credentials,
        MetricsOps.noop[IO],
        clientConfig = Some(GoogleBigQueryClient.Config(QueryClient.PollConfig(), Some(defaults))))
    } yield underlying

}
