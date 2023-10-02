# User defined functions (UDF)

## Limitations

**SQL views** does not allow us to create temporary functions, something that limits our of use them. You will need to use
persistent UDF in view.

## Defining UDF

BigQuery supports UDF written in SQL and JavaScript. 

**SQL (Temporary)**
```scala mdoc
import no.nrk.bigquery._
import no.nrk.bigquery.syntax._

object MyTemporarySQLUdfs {

  val addOneUdf = UDF.temporary(
    ident"addOneSqlUdf",
    UDF.Params(UDF.Param("n", BQType.FLOAT64)),
    UDF.Body.Sql(bqfr"""(n + 1)"""),
    Some(BQType.FLOAT64)
  )

}
```

**SQL (Persistent)**
```scala mdoc
import no.nrk.bigquery._
import no.nrk.bigquery.syntax._

object MyPersistentSQLUdfs {
  val dataset = BQDataset.unsafeOf(ProjectId.unsafeFromString("my-project"), "ds1", None)
  val addOneUdf = UDF.persistent(
    ident"addOneSqlUdf",
    dataset,
    UDF.Params(UDF.Param("n", BQType.FLOAT64)),
    UDF.Body.Sql(bqfr"""(n + 1)"""),
    Some(BQType.FLOAT64)
  )

}
```
Note: Use `EnsureUpdated` to deploy the persistent UDF to BigQuery.


**JavaScript**
```scala mdoc
import no.nrk.bigquery._
import no.nrk.bigquery.syntax._

object MyJsUdfs {

  // optional library in google cloud storage
  val jsLibraryGcsPath = List.empty
  val addOneUdf = UDF.temporary(
    ident"addOneJsUdf",
    UDF.Params(UDF.Param("n", BQType.FLOAT64)),
    UDF.Body.Js("return n + 1", jsLibraryGcsPath),
    Some(BQType.FLOAT64)
  )

}
```

## Calling UDF in queries

Like any other function we can call UDFs by passing in the required arguments. The library will inline the UDF as an 
temporary function if it's referenced in a query.

```scala mdoc
import no.nrk.bigquery._
import no.nrk.bigquery.syntax._

val n = ident"n"
val myQuery: BQSqlFrag =
  bqfr"""|select
         |  ${MyTemporarySQLUdfs.addOneUdf(n)} as sql,
         |  ${MyTemporarySQLUdfs.addOneUdf(n)}  as js
         |from unnest([1 ,2, 3]) as $n
         |""".stripMargin
```

## Testing

```scala
import io.circe.Json
import no.nrk.bigquery.testing.{BQUdfSmokeTest, BigQueryTestClient}

class ExampleUdfTest extends BQUdfSmokeTest(BigQueryTestClient.testClient) {

  bqCheckCall("add one to SQL udf", MyTemporarySQLUdfs.addOneUdf(1), Json.fromInt(2))
  bqCheckCall("add one to SQL udf", MyPersistentSQLUdfs.addOneUdf(1), Json.fromInt(2))
  bqCheckCall("add one to JS udf", MyJsUdfs.addOneUdf(1), Json.fromInt(2))

}
```