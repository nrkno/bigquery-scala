package no.nrk.bigquery

import java.time.LocalDate

/** A data type which describes the job responsible for the fills. It's opaque to the BQ code
  */
trait JobKeyBQ

case class BQFill(jobKey: JobKeyBQ, tableDef: BQTableDef.Table[LocalDate], query: BQSqlFrag, executionDate: LocalDate) {
  val destination: BQPartitionId[LocalDate] =
    tableDef.assertPartition(executionDate)
}

case class BQFilledTable(jobKey: JobKeyBQ, tableDef: BQTableDef.Table[LocalDate], query: LocalDate => BQSqlFrag) {
  def withDate(executionDate: LocalDate): BQFill =
    BQFill(jobKey, tableDef, query(executionDate), executionDate)
}
