# User defined functions (UDF)

## Limitations

The library does not support for creating and deploying global UDFs. **SQL views** does not allow us to create temporary
functions, something that limits our of use them. This *will* be addressed in a later version. 

## Defining UDF

BigQuery supports UDF written in SQL and JavaScript. 

**SQL**
```scala mdoc
import no.nrk.bigquery._
import no.nrk.bigquery.syntax._

object MySQLUdfs {

  val addOneUdf = UDF(
    ident"addOneSqlUdf",
    UDF.Param("n", BQType.FLOAT64) :: Nil,
    UDF.Body.Sql(bqfr"""(n + 1)"""),
    Some(BQType.FLOAT64)
  )

}
```

**JavaScript**
```scala mdoc
import no.nrk.bigquery._
import no.nrk.bigquery.syntax._

object MyJsUdfs {

  // optional library in google cloud storage
  val jsLibraryGcsPath = None
  val addOneUdf = UDF(
    ident"addOneJsUdf",
    UDF.Param("n", BQType.FLOAT64) :: Nil,
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
         |  ${MySQLUdfs.addOneUdf(n)} as sql,
         |  ${MyJsUdfs.addOneUdf(n)}  as js
         |from unnest([1 ,2, 3]) as $n
         |""".stripMargin
```

## Testing

```scala mdoc
import io.circe.Json
import no.nrk.bigquery.testing.{BQUdfSmokeTest, BigQueryTestClient}

class ExampleUdfTest extends BQUdfSmokeTest(BigQueryTestClient.testClient) {

  bqCheckCall("add one to SQL udf", MySQLUdfs.addOneUdf(1), Json.fromInt(2))
  bqCheckCall("add one to JS udf", MyJsUdfs.addOneUdf(1), Json.fromInt(2))

}
```