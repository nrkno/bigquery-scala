/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

/** A data type which describes the job responsible for the fills. It's opaque to the BQ code
  */
trait JobKeyBQ

case class BQFill[+P](
    jobKey: JobKeyBQ,
    tableDef: BQTableDef.Table[P],
    query: BQSqlFrag,
    partitionValue: P
)(implicit P: TableOps[P]) {
  val destination: BQPartitionId[P] =
    tableDef.assertPartition(partitionValue)
}
