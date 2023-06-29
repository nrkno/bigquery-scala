package no.nrk.bigquery

import com.google.cloud.bigquery.TableInfo

case class TableOptions(
    partitionFilterRequired: Boolean
)

object TableOptions {
  val Empty: TableOptions = TableOptions(
    partitionFilterRequired = false
  )

  def fromTableInfo(tableInfo: TableInfo): TableOptions = TableOptions(
    partitionFilterRequired = Option(tableInfo.getRequirePartitionFilter).exists(_.booleanValue())
  )
}
