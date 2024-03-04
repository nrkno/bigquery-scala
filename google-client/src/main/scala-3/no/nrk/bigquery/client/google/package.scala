/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.client.google

import com.google.cloud.bigquery.Job
import no.nrk.bigquery.QueryClient

type GoogleQueryClient[F[_]] = QueryClient.Aux[F, Job]
