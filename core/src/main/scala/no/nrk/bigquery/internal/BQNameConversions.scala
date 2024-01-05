/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.internal

import no.nrk.bigquery.{BQJobId, BQJobName, JobLabels}

trait BQNameConversions {

  implicit def bqNameToBQJobId(name: BQJobName): BQJobId = BQJobId(None, None, name, JobLabels.empty)

}
