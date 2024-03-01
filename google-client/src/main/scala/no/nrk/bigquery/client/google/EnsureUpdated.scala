/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.client.google

import cats.MonadThrow
import com.google.cloud.bigquery.{RoutineInfo, TableInfo}
import no.nrk.bigquery.internal.EnsureUpdatedBase
import org.typelevel.log4cats.LoggerFactory

class EnsureUpdated[F[_]](client: GoogleBigQueryClient[F])(implicit F: MonadThrow[F], lf: LoggerFactory[F])
    extends EnsureUpdatedBase[F, RoutineInfo, TableInfo](client)
