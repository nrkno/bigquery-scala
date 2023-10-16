package example.comExample.test

import no.nrk.bigquery._
import no.nrk.bigquery.syntax._

object SomethingImportantViewView {
  val definition = BQTableDef.View(
    tableId = BQTableId.unsafeFromString("com-example.test.something-important-view"),
    partitionType = BQPartitionType.DatePartitioned(Ident("partitionDate")),
    query = bqsql"""|select partitionDate, COUNT(*) from `com-example.test.something-important` group by 1
""".stripMargin,
    schema = BQSchema.of(
      BQField("partitionDate", BQField.Type.DATE, BQField.Mode.NULLABLE, None),
      BQField("count", BQField.Type.INT64, BQField.Mode.NULLABLE, None)
    ),
    description = Some("desc"),
    labels = TableLabels("foo" -> "bar"),
  )
}
