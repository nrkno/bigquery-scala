/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.client.http4s

import cats.MonadThrow
import googleapis.bigquery.{Routine, Table}
import no.nrk.bigquery.internal.EnsureUpdatedBase
import org.typelevel.log4cats.LoggerFactory

class EnsureUpdated[F[_]](client: Http4sBigQueryClient[F])(implicit F: MonadThrow[F], lf: LoggerFactory[F])
    extends EnsureUpdatedBase[F, Routine, Table](client)
