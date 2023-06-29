package no.nrk.bigquery.internal

import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.TimePartitioning.Type
import com.google.cloud.bigquery.{Option => _, _}
import munit.FunSuite
import no.nrk.bigquery.syntax._
import no.nrk.bigquery._

class TableUpdateOperationTest extends FunSuite {

  private val a = BQField("a", StandardSQLTypeName.INT64, Mode.REQUIRED)
  private val b = BQField("b", StandardSQLTypeName.INT64, Mode.REQUIRED)
  private val c = BQField("c", StandardSQLTypeName.INT64, Mode.REQUIRED)
  private val viewId = BQTableId.of(ProjectId("project"), "dataset", "view")
  private val tableId = BQTableId.of(ProjectId("project"), "dataset", "table")
  private val materializedViewId =
    BQTableId.of(ProjectId("project"), "dataset", "mat_view")

  test("views with schema should trigger update after create") {
    val schema = BQSchema.of(a)
    val testView = BQTableDef.View(
      viewId,
      BQPartitionType.NotPartitioned,
      bqsql"select 1 as a",
      schema,
      Some("description"),
      TableLabels.Empty
    )
    val remote = None

    TableUpdateOperation.from(testView, remote) match {
      case UpdateOperation.CreateTable(_, _, maybePatchedTable) =>
        assert(
          maybePatchedTable.nonEmpty,
          "Expected create with patched table when we have schema"
        )
      case other => fail(other.toString)
    }
  }

  test("views where remote doesnt have description should be updated") {
    val schema = BQSchema.of(a)
    val query = bqsql"select 1 as a"
    val description = "description"
    val testView = BQTableDef.View(
      viewId,
      BQPartitionType.NotPartitioned,
      query,
      schema,
      Some(description),
      TableLabels.Empty
    )
    val remote = Some(
      TableInfo
        .newBuilder(
          viewId.underlying,
          ViewDefinition
            .newBuilder(query.asStringWithUDFs)
            .setSchema(schema.toSchema)
            .build()
        )
        .build()
    )

    TableUpdateOperation.from(testView, remote) match {
      case UpdateOperation.RecreateView(from, to, createNew) =>
        assert(
          Option(from.getDescription).isEmpty,
          "Expected `from` table to not have description"
        )
        assert(
          to.description.contains(description),
          "Expected `to` to have description"
        )
        assert(
          createNew.table.getDescription == description,
          "Expected `updatedTable` to have description"
        )
      case other => fail(other.toString)
    }
  }

  test("views with schema should detect no changes") {
    val schema = BQSchema.of(a)
    val query = bqsql"select 1 as a"
    val description = "description"
    val testView = BQTableDef.View(
      viewId,
      BQPartitionType.NotPartitioned,
      query,
      schema,
      Some(description),
      TableLabels.Empty
    )
    val remote =
      Some(
        TableInfo
          .newBuilder(
            viewId.underlying,
            ViewDefinition
              .newBuilder(query.asStringWithUDFs)
              .setSchema(schema.toSchema)
              .build()
          )
          .setDescription(description)
          .build()
      )

    TableUpdateOperation.from(testView, remote) match {
      case UpdateOperation.Noop(_) =>
      case other => fail(other.toString)
    }
  }

  test("should keep unknown values set on a TableInfo for table updates") {
    val schema = BQSchema.of(a)
    val description = "description"
    val testTable = BQTableDef.Table(
      tableId,
      schema,
      BQPartitionType.NotPartitioned,
      Some(description),
      clustering = Nil,
      TableLabels.Empty
    )
    val friendlyName = "friendlyName"
    val remote = Some(
      TableInfo
        .newBuilder(
          viewId.underlying,
          StandardTableDefinition.newBuilder.setSchema(schema.toSchema).build()
        )
        .setFriendlyName(friendlyName)
        .build()
    )

    TableUpdateOperation.from(testTable, remote) match {
      case UpdateOperation.UpdateTable(_, _, updatedTable) =>
        assert(
          updatedTable.getFriendlyName == friendlyName,
          "Expected `updatedTable` to contain friendly name"
        )
      case other => fail(other.toString)
    }
  }

  test("should allow valid extension of schema") {
    val givenTable = BQTableDef.Table(
      tableId,
      BQSchema.of(a, b, c),
      BQPartitionType.NotPartitioned,
      description = None,
      clustering = Nil,
      TableLabels.Empty
    )
    val actualTable = Some(
      TableInfo
        .newBuilder(
          viewId.underlying,
          StandardTableDefinition.newBuilder
            .setSchema(BQSchema.of(a, b).toSchema)
            .build()
        )
        .build()
    )

    TableUpdateOperation.from(givenTable, actualTable) match {
      case UpdateOperation.UpdateTable(_, _, _) => assert(cond = true)
      case other => fail(other.toString)
    }
  }

  test("should not allow invalid extension of schema") {
    val givenTable = BQTableDef.Table(
      tableId,
      BQSchema.of(a, c, b),
      BQPartitionType.NotPartitioned,
      description = None,
      clustering = Nil,
      TableLabels.Empty
    )
    val actualTable = Some(
      TableInfo
        .newBuilder(
          viewId.underlying,
          StandardTableDefinition.newBuilder
            .setSchema(BQSchema.of(a, b).toSchema)
            .build()
        )
        .build()
    )

    TableUpdateOperation.from(givenTable, actualTable) match {
      case UpdateOperation.IllegalSchemaExtension(_, reason) =>
        assertEquals(reason, "Expected field `b`, got field `c`")
      case other => fail(other.toString)
    }
  }

  test("should allow valid extension of nested schema") {
    val ba = BQField.struct("b", Mode.REQUIRED)(a)
    val bac = BQField.struct("b", Mode.REQUIRED)(a, c)

    val givenTable = BQTableDef.Table(
      tableId,
      BQSchema.of(bac),
      BQPartitionType.NotPartitioned,
      description = None,
      clustering = Nil,
      TableLabels.Empty
    )
    val actualTable = Some(
      TableInfo
        .newBuilder(
          viewId.underlying,
          StandardTableDefinition.newBuilder
            .setSchema(BQSchema.of(ba).toSchema)
            .build()
        )
        .build()
    )

    TableUpdateOperation.from(givenTable, actualTable) match {
      case UpdateOperation.UpdateTable(_, _, _) => assert(cond = true)
      case other => fail(other.toString)
    }
  }

  test("should not allow invalid extension of nested schema") {
    val bab = BQField.struct("b", Mode.REQUIRED)(a, b)
    val bac = BQField.struct("b", Mode.REQUIRED)(a, c)

    val givenTable = BQTableDef.Table(
      tableId,
      BQSchema.of(bac),
      BQPartitionType.NotPartitioned,
      description = None,
      clustering = Nil,
      TableLabels.Empty
    )
    val actualTable = Some(
      TableInfo
        .newBuilder(
          viewId.underlying,
          StandardTableDefinition.newBuilder
            .setSchema(BQSchema.of(bab).toSchema)
            .build()
        )
        .build()
    )

    TableUpdateOperation.from(givenTable, actualTable) match {
      case UpdateOperation.IllegalSchemaExtension(_, reason) =>
        assertEquals(reason, "Expected field `b.b`, got field `b.c`")
      case other => fail(other.toString)
    }
  }

  test("should recreate MV on changes") {
    val query = bqsql"select 1 as a"
    val description = "description"
    val testView = BQTableDef.MaterializedView(
      materializedViewId,
      BQPartitionType.NotPartitioned,
      query,
      BQSchema.of(),
      enableRefresh = true,
      180000,
      Some(description),
      TableLabels.Empty
    )
    val friendlyName = "friendlyName"
    val expirationTime: java.lang.Long = 100L
    val remote = Some(
      TableInfo
        .newBuilder(
          materializedViewId.underlying,
          MaterializedViewDefinition
            .newBuilder(query.asStringWithUDFs)
            .setSchema(testView.schema.toSchema)
            .build()
        )
        .setFriendlyName(friendlyName)
        .setExpirationTime(expirationTime)
        .build()
    )

    TableUpdateOperation.from(testView, remote) match {
      case UpdateOperation.RecreateView(_, _, _) => assert(cond = true)
      case other => fail(other.toString)
    }
  }

  test("should fail when faced with unrecognized partition scheme") {
    val testTable = BQTableDef.Table(
      tableId,
      BQSchema.of(a),
      BQPartitionType.NotPartitioned,
      description = None,
      clustering = Nil,
      TableLabels.Empty
    )
    val remote = Some(
      TableInfo
        .newBuilder(
          viewId.underlying,
          StandardTableDefinition.newBuilder
            .setSchema(BQSchema.of(a).toSchema)
            .setTimePartitioning(TimePartitioning.of(Type.HOUR))
            .build()
        )
        .build()
    )

    TableUpdateOperation.from(testTable, remote) match {
      case UpdateOperation.UnsupportedPartitioning(_, msg) =>
        assertEquals(
          msg,
          "Need to implement support in `BQPartitionType` for Some(TimePartitioning{type=HOUR, expirationMs=null, field=null, requirePartitionFilter=null})"
        )
      case other => fail(other.toString)
    }
  }

  test("should fail when faced with different partition scheme") {
    val testTable = BQTableDef.Table(
      tableId,
      BQSchema.of(a),
      BQPartitionType.NotPartitioned,
      description = None,
      clustering = Nil,
      TableLabels.Empty
    )
    val remote = Some(
      TableInfo
        .newBuilder(
          viewId.underlying,
          StandardTableDefinition.newBuilder
            .setSchema(BQSchema.of(a).toSchema)
            .setTimePartitioning(
              TimePartitioning.newBuilder(Type.DAY).setField("date").build()
            )
            .build()
        )
        .build()
    )

    TableUpdateOperation.from(testTable, remote) match {
      case UpdateOperation.UnsupportedPartitioning(_, msg) =>
        assertEquals(
          msg,
          "Cannot change partitioning from DatePartitioned(Ident(date)) to NotPartitioned"
        )
      case other => fail(other.toString)
    }
  }

  test("schema changes must maintain order") {
    val testTable = BQTableDef.Table(
      tableId,
      BQSchema.of(a, b, c),
      BQPartitionType.NotPartitioned,
      description = None,
      clustering = Nil,
      TableLabels.Empty
    )
    val remote = Some(
      TableInfo
        .newBuilder(
          viewId.underlying,
          StandardTableDefinition.newBuilder
            .setSchema(BQSchema.of(a, c, b).toSchema)
            .build()
        )
        .build()
    )

    TableUpdateOperation.from(testTable, remote) match {
      case UpdateOperation.IllegalSchemaExtension(_, reason) =>
        assertEquals(
          reason,
          "Expected field `c`, got field `b`, Expected field `b`, got field `c`"
        )
      case other => fail(other.toString)
    }
  }

  test("updating partitionFilterRequired should result in update") {
    def testTable(filter: Boolean) = BQTableDef.Table(
      tableId,
      BQSchema.of(a),
      BQPartitionType.DatePartitioned(Ident("date")),
      description = None,
      clustering = Nil,
      TableLabels.Empty,
      tableOptions = TableOptions(partitionFilterRequired = filter)
    )
    def remote(filter: Boolean) = Some(
      TableInfo
        .newBuilder(
          tableId.underlying,
          StandardTableDefinition.newBuilder
            .setSchema(BQSchema.of(a).toSchema)
            .setTimePartitioning(
              TimePartitioning
                .newBuilder(Type.DAY)
                .setField("date")
                .build()
            )
            .build()
        )
        .setRequirePartitionFilter(filter)
        .build()
    )

    TableUpdateOperation.from(testTable(true), remote(false)) match {
      case UpdateOperation.UpdateTable(_, _, table) =>
        assert(table.getRequirePartitionFilter)
      case other => fail(other.toString)
    }
    TableUpdateOperation.from(testTable(false), remote(true)) match {
      case UpdateOperation.UpdateTable(_, _, table) =>
        assert(!table.getRequirePartitionFilter)
      case other => fail(other.toString)
    }
    TableUpdateOperation.from(testTable(true), remote(true)) match {
      case UpdateOperation.Noop(_) =>
      case other => fail(other.toString)
    }

  }

}
