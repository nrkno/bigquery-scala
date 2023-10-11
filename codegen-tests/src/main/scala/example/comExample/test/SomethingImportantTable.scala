package example.comExample.test

import no.nrk.bigquery._
import no.nrk.bigquery.syntax._

object SomethingImportantTable {
  val definition = BQTableDef.Table(
    tableId = BQTableId.unsafeFromString("com-example.test.something-important"),
    schema = BQSchema.of(
      BQField("partitionDate", BQField.Type.DATE, BQField.Mode.REQUIRED, None),
      BQField("name", BQField.Type.STRING, BQField.Mode.REQUIRED, None),
      BQField("list", BQField.Type.INT64, BQField.Mode.REPEATED, None),
      BQField.struct("complex", BQField.Mode.NULLABLE, None)(
        BQField("one", BQField.Type.STRING, BQField.Mode.REQUIRED, None),
        BQField("two", BQField.Type.STRING, BQField.Mode.REQUIRED, None),
        BQField("three", BQField.Type.BOOL, BQField.Mode.NULLABLE, None)
      )
    ),
    partitionType = BQPartitionType.DatePartitioned(Ident("partitionDate")),
    description = Some("desc"),
    clustering = List(),
    labels = TableLabels("foo" -> "bar"),
    tableOptions = TableOptions.Empty
  )
}
