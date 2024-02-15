# Table value function (TVF)

## Defining a TVF

```scala mdoc
import no.nrk.bigquery._
import no.nrk.bigquery.syntax._
import no.nrk.bigquery.util._

object MyTvf {
  val schema: BQSchema = BQSchema.of(
    BQField("id", BQField.Type.STRING, BQField.Mode.NULLABLE)
  )
  val dataset: BQDataset.Ref = BQDataset.Ref.unsafeOf(ProjectId.unsafeFromString("some-project"), "my_dataset")
  val sourceTable = BQTableDef.Table(
    BQTableId.unsafeOf(dataset, "source-table"),
    schema,
    BQPartitionType.NotPartitioned
  )
  val tvf: TVF[Unit, nat._1] = TVF(
    TVF.TVFId(dataset, ident"my_tvf"),
    BQPartitionType.NotPartitioned,
    BQRoutine.Params(BQRoutine.Param("myId", BQType.STRING)),
    bqfr"select id from $sourceTable where id = myId",
    schema,
    None
  )

}
```
Note: Use `EnsureUpdated` to deploy TVFs to BigQuery.

## Calling TVF in queries

Calling an TVF is quite equal as calling UDFs. The main difference it that it returns a table and not a value.

```scala mdoc
import no.nrk.bigquery._
import no.nrk.bigquery.syntax._

val n = ident"n"
val myQuery: BQSqlFrag =
  bqfr"""|select *
         |from ${MyTvf.tvf(n)}
         |""".stripMargin
```

## Testing

```scala
import io.circe.Json
import no.nrk.bigquery.testing.{BQUdfSmokeTest, BigQueryTestClient}

class ExampleTvfTest extends BQUdfSmokeTest(BigQueryTestClient.testClient) {

  bqCheckTableValueFunction("my_tvf", MyTvftvf)(args => args(StringValue("1")))
  
}
```