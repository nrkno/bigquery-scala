/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.internal

import no.nrk.bigquery.BQDataset
import scala.language.implicitConversions

trait ConversionSyntax {
  implicit def toBQDatasetRef(ds: BQDataset): BQDataset.Ref = ds.toRef
}
