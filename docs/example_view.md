# View

## Table Schemas

To start of we need to define some tables we can query. The schema DSL is inspired by the BigQuery table json definition.

Here we have to tables, `my-gcp-project.prod.user_log` and `my-gcp-project.prod.users`

```scala mdoc
import no.nrk.bigquery._
import no.nrk.bigquery.syntax._
import no.nrk.bigquery.util.nat._1
import java.time.LocalDate

object Schemas {

  object UserEventSchema {
    private val timestamp: BQField = BQField("timestamp", BQField.Type.TIMESTAMP, BQField.Mode.REQUIRED)
    val tableDef: BQTableDef.Table[LocalDate] = BQTableDef.Table(
      BQTableId.unsafeOf(BQDataset.unsafeOf(ProjectId.unsafeFromString("my-gcp-project"), "prod", Some(LocationId.EU)), "user_log"),
      BQSchema.of(
        BQField("eventId", BQField.Type.STRING, BQField.Mode.REQUIRED),
        timestamp,
        BQField("userId", BQField.Type.STRING, BQField.Mode.REQUIRED),
        BQField.struct("activity", BQField.Mode.REQUIRED)(
          BQField("type", BQField.Type.INT64, BQField.Mode.REQUIRED),
          BQField("value", BQField.Type.STRING, BQField.Mode.NULLABLE)
        )
      ),
      BQPartitionType.DatePartitioned(timestamp.ident)
    )
  }

  object UserSchema {
    private val namesStruct: BQField = BQField.struct("names", BQField.Mode.REQUIRED)(
      BQField("firstName", BQField.Type.INT64, BQField.Mode.REQUIRED),
      BQField("middleName", BQField.Type.STRING, BQField.Mode.NULLABLE),
      BQField("lastName", BQField.Type.STRING, BQField.Mode.REQUIRED)
    )
    val tableDef: BQTableDef.Table[Unit] = BQTableDef.Table(
      BQTableId.unsafeOf(BQDataset.unsafeOf(ProjectId.unsafeFromString("my-gcp-project"), "prod", Some(LocationId.EU)), "users"),
      BQSchema.of(
        BQField("userId", BQField.Type.STRING, BQField.Mode.REQUIRED),
        namesStruct
      ),
      BQPartitionType.NotPartitioned
    )

    val fullNameUdf: UDF.Temporary[_1] = UDF.temporary(
      ident"toFullName",
      UDF.Params(UDF.Param.fromField(namesStruct)),
      UDF.Body.Sql(
        bqfr"""(names.firstName || ' ' || coalesce(names.middleName || ' ', '') || names.lastName)""".stripMargin
      ),
      Some(BQType.STRING)
    )

  }
}
```

Now we can use the schema definitions to write up a query.

## Construct a view

In this example we join in the user names and normalize the struct values.

```scala mdoc
import no.nrk.bigquery._
import Schemas._
import no.nrk.bigquery.syntax._

object UserEventView {

  val query: BQSqlFrag =
    bqfr"""|select
           |  event.timestamp,
           |  event.userId,
           |  (user.names.firstName || ' ' || user.names.lastName) as fullName,
           |  event.activity.type as activityType,
           |  event.activity.value as activityValue
           |from ${UserEventSchema.tableDef.unpartitioned} event
           |join ${UserSchema.tableDef.unpartitioned} user on user.userId = event.userId
           |where event.activity.value is not null
           |""".stripMargin

  private val timestamp: BQField = BQField("timestamp", BQField.Type.TIMESTAMP, BQField.Mode.REQUIRED)

  val viewDef: BQTableDef.View[LocalDate] = BQTableDef.View(
    BQTableId.unsafeOf(BQDataset.unsafeOf(ProjectId.unsafeFromString("my-gcp-project"), "prod", Some(LocationId.EU)), "user_activity_view"),
    BQPartitionType.DatePartitioned(timestamp.ident),
    query,
    BQSchema.of(
      timestamp,
      BQField("userId", BQField.Type.STRING, BQField.Mode.REQUIRED),
      BQField("fullName", BQField.Type.STRING, BQField.Mode.REQUIRED),
      BQField("activityType", BQField.Type.INT64, BQField.Mode.REQUIRED),
      BQField("activityValue", BQField.Type.STRING, BQField.Mode.REQUIRED)
    )
  )

}
```

Note: Use `EnsureUpdated` to deploy the view to BiqQuery

## Testing

Given the view definition and the SQL query above we can render the queries that BiqQuery can validate for us. The result
will be cached in a `generated` folder that should be checked into version control. The test framework checks the rendered
version against the generated folder to determine the test it need to rerun using BigQuery. This make it possible to quickly
run all tests without getting in to issues like api quotas or cost issued.

```scala
import no.nrk.bigquery.testing.{BQSmokeTest, BigQueryTestClient}

class UserEventViewTest extends BQSmokeTest(BigQueryTestClient.testClient) {

  bqCheckViewTest("user-event-view", UserEventView.viewDef)

}
```