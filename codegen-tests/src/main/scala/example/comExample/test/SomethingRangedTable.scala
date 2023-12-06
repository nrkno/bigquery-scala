package example.comExample.test

import no.nrk.bigquery._
import no.nrk.bigquery.syntax._

object SomethingRangedTable {
  val definition = BQTableDef.Table(
    tableId = BQTableId.unsafeFromString("com-example.test.something-ranged"),
    schema = BQSchema.of(
      BQField("id", BQField.Type.STRING, BQField.Mode.REQUIRED, None),
      BQField("name", BQField.Type.STRING, BQField.Mode.REQUIRED, None)
    ),
    partitionType = BQPartitionType.IntegerRangePartitioned(Ident("id"), BQIntegerRange(start = 0, end = 4000, interval = 1)),
    description = Some("desc"),
    clustering = List(),
    labels = TableLabels("foo" -> "bar"),
    tableOptions = TableOptions.Empty
  )
}
