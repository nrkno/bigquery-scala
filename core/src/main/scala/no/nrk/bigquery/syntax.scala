/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package no.nrk.bigquery

import no.nrk.bigquery.internal.{BQLiteralSyntax, BQShowSyntax}

object syntax extends BQLiteralSyntax with BQShowSyntax
