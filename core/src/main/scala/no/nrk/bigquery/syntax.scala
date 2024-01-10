/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import no.nrk.bigquery.internal.{BQLiteralSyntax, BQShowSyntax, ConversionSyntax}

object syntax extends BQLiteralSyntax with BQShowSyntax with ConversionSyntax
