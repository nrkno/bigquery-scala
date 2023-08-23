# Struct projection

Using structs in bigquery is a nice way to organize your data. However, it can be cumbersome to manup with. Struct
projection gives us a more declarative API to rewrite them. 

## Projection example

Let's start with the source struct we want to rewrite from:
```scala mdoc
import no.nrk.bigquery._
import no.nrk.bigquery.syntax._
import no.nrk.bigquery.util.BqSqlProjection
import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.StandardSQLTypeName

val originalStruct: BQField = BQField.struct("foo", Mode.NULLABLE)(
  BQField("keep_me", StandardSQLTypeName.STRING, Mode.NULLABLE),
  BQField("drop_me", StandardSQLTypeName.STRING, Mode.NULLABLE),
  BQField("rename_me", StandardSQLTypeName.STRING, Mode.NULLABLE),
  BQField.struct("keep_original_struct", Mode.NULLABLE)(
    BQField("one", StandardSQLTypeName.STRING, Mode.NULLABLE),
    BQField("two", StandardSQLTypeName.STRING, Mode.NULLABLE)
  ),
  BQField.struct("flatten_struct", Mode.NULLABLE)(
    BQField("one", StandardSQLTypeName.STRING, Mode.NULLABLE),
    BQField("two", StandardSQLTypeName.STRING, Mode.NULLABLE)
  )
)
```

In this example we have named them based on the action we want to project on them. The resulting
struct should be:

```scala mdoc
val projectedStruct: BQField = BQField.struct("foo", Mode.NULLABLE)(
  BQField("keep_me", StandardSQLTypeName.STRING, Mode.NULLABLE),
  BQField("renamed", StandardSQLTypeName.STRING, Mode.NULLABLE),
  BQField.struct("keep_original_struct", Mode.NULLABLE)(
    BQField("one", StandardSQLTypeName.STRING, Mode.NULLABLE),
    BQField("two", StandardSQLTypeName.STRING, Mode.NULLABLE)
  ),
  BQField("barOne", StandardSQLTypeName.STRING, Mode.NULLABLE),
  BQField("barTwo", StandardSQLTypeName.STRING, Mode.NULLABLE)
)
```

The projection:
```scala mdoc
val fooProjection = BqSqlProjection(originalStruct) {
  case BQField("drop_me", _, _, _, _, _) => BqSqlProjection.Drop
  case BQField("rename_me", _, _, _, _, _) => BqSqlProjection.Rename(ident"renamed")
  case BQField("flatten_struct", _, _, _, _, _) => BqSqlProjection.Flatten(Some(ident"bar"))
  case _ => BqSqlProjection.Keep
}.get
```

## Practical use case

Being able to move forward and introduce breaking changes is a must. Doing so by back porting the latest table using
a view can be a good mechanism to do so. 

Let's start with the source table:
```scala mdoc
val originTable: BQTableDef.Table[Unit] =
  BQTableDef.Table(
    BQTableId(BQDataset(ProjectId("p1"), "d1", None), "table_1"),
    BQSchema.of(originalStruct),
    BQPartitionType.NotPartitioned
  )
```

And a query that 
```scala mdoc
val query: BQSqlFrag =
  bqfr"""|select
         |  ${fooProjection.fragment} as foo
         |from $originTable
         |""".stripMargin
```

The view and a test case for it:
```scala mdoc
import no.nrk.bigquery.testing.{BQSmokeTest, BigQueryTestClient}

val view1: BQTableDef.View[Unit] =
  BQTableDef.View(
    BQTableId(BQDataset(ProjectId("p1"), "d1", None), "view_1"),
    BQPartitionType.NotPartitioned,
    query,
    BQSchema.of(projectedStruct)
  )

class View1Test extends BQSmokeTest(BigQueryTestClient.testClient) {
  bqCheckViewTest("project a struct 2", view1)
}
```

The generated code will look something like:
```sql
select
  (SELECT AS STRUCT # start struct foo (dropped drop_me)
  foo.keep_me,
  foo.rename_me renamed,
  foo.keep_original_struct keep_original_struct,
  foo.flatten_struct.one barOne,
  foo.flatten_struct.two barTwo
) as foo
from `p1.d1.table_1`
```
