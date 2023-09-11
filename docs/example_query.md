# Query

## Table Schemas

To start of we need to define some tables we can query. The schema DSL is inspired by the BigQuery table json definition.

Here we have to tables, `my-gcp-project.prod.user_log` and `my-gcp-project.prod.users`

```scala mdoc
import no.nrk.bigquery._
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
      BQTableId.unsafeOf(BQDataset.unsafeOf(ProjectId.unsafeFromString("my-gcp-project"), "prod").withLocation(LocationId.EU), "users"),
      BQSchema.of(
        BQField("userId", BQField.Type.STRING, BQField.Mode.REQUIRED),
        namesStruct
      ),
      BQPartitionType.NotPartitioned
    )

  }
}
```

Now we can use the schema definitions to write up a query.

## Construct a query

In this example we have one table that uses daily partition where it's joined with a unpartition table. Note that
we do not need to do any escaping or formatting of the values. 

```scala mdoc
import no.nrk.bigquery._
import no.nrk.bigquery.syntax._
import java.time.LocalDate
import Schemas._

object UserEventQuery {

  def daily(day: LocalDate): BQQuery[UserActivityRow] = BQQuery(
    bqfr"""|select
           |  event.userId,
           |  array_agg(event.activity) as activities
           |from ${UserEventSchema.tableDef.assertPartition(day)} event
           |join ${UserSchema.tableDef.unpartitioned} user on user.userId = event.userId
           |group by 1, 2
           |""".stripMargin
  )

  case class Activity(
    tpe: Long,
    value: Option[String]
  )

  object Activity {
    implicit val read: BQRead[Activity] = BQRead.derived
  }

  case class UserActivityRow(
    userId: String,
    name: String,
    activities: List[Activity]
  )

  object UserActivityRow {
    implicit val read: BQRead[UserActivityRow] = BQRead.derived
  }
}
```

## Working with SQL fragments

A common thing to do with bigger programs is to spit them out into smaller part. We then combine them together when needed.
You can do that by using `BQSqlFrag` through the string interpolation `bqfr"/* my sql code */"`. Combining them is as easy as
using the string interpolation that you probably already have using in Scala.

Here's an example where we reuse a base query but let the caller define the fields it wants to extract. The queries does
have different filters.

```scala mdoc
import cats.data.NonEmptyList
import no.nrk.bigquery._
import no.nrk.bigquery.syntax._
import Schemas.{UserEventSchema, UserSchema}

object CombineQueries {

  private def baseQuery(idents: NonEmptyList[Ident]): BQSqlFrag = {
    val fields = idents.toList.map(_.bqShow).mkFragment(", ")
    bqfr"""|select $fields
           |from ${UserEventSchema.tableDef.unpartitioned} event
           |join ${UserSchema.tableDef.unpartitioned} user on user.userId = event.userId
           |""".stripMargin
  }

  private val middleNameFilter = bqfr"user.names.middleName is not null"

  private object Idents {
    val userId: Ident = ident"event.userId"
    val activity: Ident = ident"event.activity"
    val activityType: Ident = ident"event.activity.type"
    val activityValue: Ident = ident"event.activity.value"
  }

  def queryForUserId(userId: String): BQSqlFrag =
    bqfr"""|${baseQuery(NonEmptyList.of(Idents.userId, Idents.activity))}
           |where ${Idents.userId} = ${StringValue(userId)}
           |""".stripMargin

  def normalizedQueryForUserIdAndActivityTypeWithMiddleName(userId: String, activityType: Long): BQSqlFrag =
    bqfr"""|${baseQuery(NonEmptyList.of(Idents.userId, Idents.activityType, Idents.activityValue))}
           |where ${Idents.userId} = ${StringValue(userId)}
           |and ${Idents.activityValue} = $activityType
           |and $middleNameFilter
           |""".stripMargin
}
```

## Testing

Given the schema definition and the SQL query above we can render the queries that BiqQuery can validate for us. The result
will be cached in a `generated` folder that should be checked into version control. The test framework checks the rendered
version against the generated folder to determine the test it need to rerun using BigQuery. This make it possible to quickly
run all tests without getting in to issues like api quotas or cost issued.

Note that we can create illegal queries using `BQSqlFrag`s so it's essential that we write tests for them.

```scala
import no.nrk.bigquery.testing.{BQSmokeTest, BigQueryTestClient}
import java.time.LocalDate

class UserEventQueryTest extends BQSmokeTest(BigQueryTestClient.testClient) {

  bqTypeCheckTest("user-events-query") {
    UserEventQuery.daily(LocalDate.now())
  }
}
```