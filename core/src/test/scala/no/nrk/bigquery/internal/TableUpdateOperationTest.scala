/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.internal

import com.google.cloud.bigquery.TimePartitioning.Type
import com.google.cloud.bigquery.{Option => _, _}
import munit.FunSuite
import no.nrk.bigquery.syntax._
import no.nrk.bigquery._
import GoogleTypeHelper._

class TableUpdateOperationTest extends FunSuite {

  private val a = BQField("a", BQField.Type.INT64, BQField.Mode.REQUIRED)
  private val b = BQField("b", BQField.Type.INT64, BQField.Mode.REQUIRED)
  private val c = BQField("c", BQField.Type.INT64, BQField.Mode.REQUIRED)
  private val viewId = BQTableId.unsafeOf(BQDataset.unsafeOf(ProjectId("project"), "dataset"), "view")
  private val tableId = BQTableId.unsafeOf(BQDataset.unsafeOf(ProjectId("project"), "dataset"), "table")
  private val tableIdWithLocation =
    BQTableId.unsafeOf(BQDataset.unsafeOf(ProjectId("project"), "dataset", Some(LocationId.EuropeNorth1)), "table")
  private val materializedViewId =
    BQTableId.unsafeOf(BQDataset.unsafeOf(ProjectId("project"), "dataset"), "mat_view")

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
            .setSchema(SchemaHelper.toSchema(schema))
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
              .setSchema(SchemaHelper.toSchema(schema))
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
          StandardTableDefinition.newBuilder.setSchema(SchemaHelper.toSchema(schema)).build()
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
            .setSchema(SchemaHelper.toSchema(BQSchema.of(a, b)))
            .build()
        )
        .build()
    )

    TableUpdateOperation.from(givenTable, actualTable) match {
      case UpdateOperation.UpdateTable(_, _, _) => assert(cond = true)
      case other => fail(other.toString)
    }
  }

  test("should be a noop when no fields has changed") {
    val givenTable = BQTableDef.Table(
      tableIdWithLocation,
      BQSchema.of(a, b),
      BQPartitionType.NotPartitioned,
      description = None,
      clustering = Nil,
      TableLabels.Empty
    )
    val actualTable = Some(
      TableInfo
        .newBuilder(
          tableIdWithLocation.underlying,
          StandardTableDefinition.newBuilder
            .setSchema(SchemaHelper.toSchema(BQSchema.of(a, b)))
            .setLocation(LocationId.EuropeNorth1.value)
            .build()
        )
        .build()
    )

    TableUpdateOperation.from(givenTable, actualTable) match {
      case UpdateOperation.Noop(_) => assert(cond = true)
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
            .setSchema(SchemaHelper.toSchema(BQSchema.of(a, b)))
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
    val ba = BQField.struct("b", BQField.Mode.REQUIRED)(a)
    val bac = BQField.struct("b", BQField.Mode.REQUIRED)(a, c)

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
            .setSchema(SchemaHelper.toSchema(BQSchema.of(ba)))
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
    val bab = BQField.struct("b", BQField.Mode.REQUIRED)(a, b)
    val bac = BQField.struct("b", BQField.Mode.REQUIRED)(a, c)

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
            .setSchema(SchemaHelper.toSchema(BQSchema.of(bab)))
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
            .setSchema(SchemaHelper.toSchema(testView.schema))
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
            .setSchema(SchemaHelper.toSchema(BQSchema.of(a)))
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
            .setSchema(SchemaHelper.toSchema(BQSchema.of(a)))
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
            .setSchema(SchemaHelper.toSchema(BQSchema.of(a, c, b)))
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
    def remote(filter: Option[Boolean]) = Some(
      TableInfo
        .newBuilder(
          tableId.underlying,
          StandardTableDefinition.newBuilder
            .setSchema(SchemaHelper.toSchema(BQSchema.of(a)))
            .setTimePartitioning(
              TimePartitioning
                .newBuilder(Type.DAY)
                .setField("date")
                .build()
            )
            .build()
        )
        .setRequirePartitionFilter(filter.map(Boolean.box).orNull)
        .build()
    )

    TableUpdateOperation.from(testTable(true), remote(Some(false))) match {
      case UpdateOperation.UpdateTable(_, _, table) =>
        assert(table.getRequirePartitionFilter)
      case other => fail(other.toString)
    }
    TableUpdateOperation.from(testTable(false), remote(Some(true))) match {
      case UpdateOperation.UpdateTable(_, _, table) =>
        assert(!table.getRequirePartitionFilter)
      case other => fail(other.toString)
    }
    TableUpdateOperation.from(testTable(true), remote(Some(true))) match {
      case UpdateOperation.Noop(_) =>
      case other => fail(other.toString)
    }
    TableUpdateOperation.from(testTable(false), remote(None)) match {
      case UpdateOperation.Noop(_) =>
      case other => fail(other.toString)
    }
  }

}
