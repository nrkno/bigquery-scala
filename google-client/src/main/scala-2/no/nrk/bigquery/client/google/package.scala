/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.client

import com.google.cloud.bigquery.Job
import no.nrk.bigquery.QueryClient

package object google {
  type GoogleQueryClient[F[_]] = QueryClient.Aux[F, Job]
}
