/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import no.nrk.bigquery.internal.{BQLiteralSyntax, BQNameConversions, BQShowSyntax}

object syntax extends BQLiteralSyntax with BQShowSyntax with BQNameConversions
