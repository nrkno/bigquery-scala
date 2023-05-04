# View

## Table Schemas

To start of we need to define some tables we can query. The schema DSL is inspired by the BigQuery table json definition.

Here we have to tables, `my-gcp-project.prod.user_log` and `my-gcp-project.prod.users`

```scala mdoc
import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.StandardSQLTypeName
import no.nrk.bigquery._
import no.nrk.bigquery.syntax._
import java.time.LocalDate

object Schemas {

  object UserEventSchema {
    private val timestamp: BQField = BQField("timestamp", StandardSQLTypeName.TIMESTAMP, Mode.REQUIRED)
    val tableDef: BQTableDef.Table[LocalDate] = BQTableDef.Table(
      BQTableId(BQDataset(ProjectId("my-gcp-project"), "prod", Some(LocationId("eu"))), "user_log"),
      BQSchema.of(
        BQField("eventId", StandardSQLTypeName.STRING, Mode.REQUIRED),
        timestamp,
        BQField("userId", StandardSQLTypeName.STRING, Mode.REQUIRED),
        BQField.struct("activity", Mode.REQUIRED)(
          BQField("type", StandardSQLTypeName.INT64, Mode.REQUIRED),
          BQField("value", StandardSQLTypeName.STRING, Mode.NULLABLE)
        )
      ),
      BQPartitionType.DatePartitioned(timestamp.ident)
    )
  }

  object UserSchema {
    private val namesStruct: BQField = BQField.struct("names", Mode.REQUIRED)(
      BQField("firstName", StandardSQLTypeName.INT64, Mode.REQUIRED),
      BQField("middleName", StandardSQLTypeName.STRING, Mode.NULLABLE),
      BQField("lastName", StandardSQLTypeName.STRING, Mode.REQUIRED)
    )
    val tableDef: BQTableDef.Table[Unit] = BQTableDef.Table(
      BQTableId(BQDataset(ProjectId("my-gcp-project"), "prod", Some(LocationId("eu"))), "users"),
      BQSchema.of(
        BQField("userId", StandardSQLTypeName.STRING, Mode.REQUIRED),
        namesStruct
      ),
      BQPartitionType.NotPartitioned
    )

    val fullNameUdf: UDF = UDF(
      Ident("toFullName"),
      UDF.Param.fromField(namesStruct) :: Nil,
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
import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.StandardSQLTypeName
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

  private val timestamp: BQField = BQField("timestamp", StandardSQLTypeName.TIMESTAMP, Mode.REQUIRED)

  val viewDef: BQTableDef.View[LocalDate] = BQTableDef.View(
    BQTableId(BQDataset(ProjectId("my-gcp-project"), "prod", Some(LocationId("eu"))), "user_activity_view"),
    BQPartitionType.DatePartitioned(timestamp.ident),
    query,
    BQSchema.of(
      timestamp,
      BQField("userId", StandardSQLTypeName.STRING, Mode.REQUIRED),
      BQField("fullName", StandardSQLTypeName.STRING, Mode.REQUIRED),
      BQField("activityType", StandardSQLTypeName.INT64, Mode.REQUIRED),
      BQField("activityValue", StandardSQLTypeName.STRING, Mode.REQUIRED)
    )
  )

}

```

## Testing

Given the view definition and the SQL query above we can render the queries that BiqQuery can validate for us. The result
will be cached in a `generated` folder that should be checked into version control. The test framework checks the rendered
version against the generated folder to determine the test it need to rerun using BigQuery. This make it possible to quickly
run all tests without getting in to issues like api quotas or cost issued.

```scala mdoc
import no.nrk.bigquery.testing.{BQSmokeTest, BigQueryTestClient}

class UserEventViewTest extends BQSmokeTest(BigQueryTestClient.testClient) {

  bqCheckViewTest("user-event-view", UserEventView.viewDef)

}
```