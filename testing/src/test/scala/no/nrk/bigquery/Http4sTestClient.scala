/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.data.OptionT
import cats.effect.{IO, Resource}
import com.permutive.gcp.auth.TokenProvider
import no.nrk.bigquery.client.http4s.Http4sBigQueryClient
import org.http4s.netty.client.NettyClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jFactory

object Http4sTestClient {
  private implicit val loggerFactory: Slf4jFactory[IO] = Slf4jFactory.create[IO]

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
      client <- NettyClientBuilder[IO].withHttp2.resource
      credentials <- Resource.eval(
        OptionT(IO(sys.env.get("BIGQUERY_SERVICE_ACCOUNT")))
          .semiflatMap { env =>
            Http4sBigQueryClient.serviceAccountFromString[IO](env, client)
          }
          .getOrElseF(TokenProvider.userAccount[IO](client)))

      underlying <- Http4sBigQueryClient.resource[IO](
        client,
        defaults,
        credentials
      )
    } yield underlying

}
