/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.internal

import com.google.cloud.bigquery.{RoutineId, RoutineInfo, StandardSQLDataType, StandardSQLField, StandardSQLTableType}
import munit.FunSuite
import no.nrk.bigquery.UpdateOperation.{CreatePersistentUdf, Illegal, UpdatePersistentUdf}
import no.nrk.bigquery._
import no.nrk.bigquery.BQRoutine.{Param, Params}
import no.nrk.bigquery.syntax._
import no.nrk.bigquery.util.nat._

import scala.jdk.CollectionConverters._

class RoutineUpdateOperationTest extends FunSuite {

  private val udf: UDF.Persistent[_0] =
    UDF.persistent(
      ident"foo",
      BQDataset.Ref(ProjectId("p1"), "ds1"),
      Params.empty,
      UDF.Body.Sql(bqfr"(1)"),
      Some(BQType.INT64))
  private val routineId: RoutineId = RoutineId.of("p1", "ds1", "foo")

  test("should create when it does not exist") {
    RoutineUpdateOperation.from(udf, None) match {
      case CreatePersistentUdf(_, routine) =>
        assertEquals(routine.getRoutineId, routineId)
      case other => fail(other.toString)
    }
  }

  test("should update udf when changed") {
    val existingRoutine = RoutineUpdateOperation.from(udf, None) match {
      case CreatePersistentUdf(_, routine) => routine
      case other => fail(s"test setup failed: ${other.toString}")
    }

    RoutineUpdateOperation.from(
      udf.copy(params = Params(Param(ident"foo", Some(BQType.INT64)))),
      Some(existingRoutine)
    ) match {
      case UpdatePersistentUdf(newUdf, newRoutine) =>
        assertNotEquals(newUdf.params.unsized.toList, udf.params.unsized.toList)
        assertNotEquals(newRoutine, existingRoutine)
      case other => fail(other.toString)
    }
  }

  test("should not attempt to update non udf routine types") {
    val routine = RoutineInfo
      .newBuilder(routineId)
      .setRoutineType("PROCEDURE")
      .build()

    RoutineUpdateOperation.from(udf, Some(routine)) match {
      case Illegal(_, _) =>
      case other => fail(other.toString)
    }
  }

  test("udf with repeated struct") {
    val udf: UDF.Persistent[_1] =
      UDF.persistent(
        ident"foo",
        BQDataset.Ref(ProjectId("p1"), "ds1"),
        Params(
          Param(
            "segments",
            BQType.fromField(
              BQField.repeatedStruct("segments")(BQField("foo", BQField.Type.STRING, BQField.Mode.REQUIRED))))),
        UDF.Body.Sql(bqfr"(1)"),
        Some(BQType.INT64)
      )

    val existingRoutine = RoutineUpdateOperation.from(udf, None) match {
      case CreatePersistentUdf(_, routine) => routine
      case other => fail(s"test setup failed: ${other.toString}")
    }

    assertEquals(existingRoutine.getArguments.get(0).getDataType.getTypeKind, "ARRAY")
  }

  test("noop udf") {
    val routine = RoutineInfo
      .newBuilder(routineId)
      .setRoutineType("SCALAR_FUNCTION")
      .setLanguage("SQL")
      .setBody("((1))")
      .setReturnType(StandardSQLDataType.newBuilder().setTypeKind(BQType.INT64.tpe.name).build())
      .build()

    RoutineUpdateOperation.from(udf, Some(routine)) match {
      case _: UpdateOperation.Noop =>
      case other => fail(other.toString)
    }
  }

  test("noop tvf") {
    val routine = RoutineInfo
      .newBuilder(routineId)
      .setRoutineType("TABLE_VALUED_FUNCTION")
      .setLanguage("SQL")
      .setBody("select 1 as n")
      .setReturnTableType(
        StandardSQLTableType
          .newBuilder()
          .setColumns(
            List(
              StandardSQLField
                .newBuilder()
                .setName("n")
                .setDataType(StandardSQLDataType.newBuilder().setTypeKind(BQType.INT64.tpe.name).build())
                .build()
            ).asJava
          )
          .build()
      )
      .build()

    val tvf = TVF(
      TVF.TVFId(BQDataset.Ref(ProjectId("p1"), "ds1"), ident"foo"),
      BQPartitionType.NotPartitioned,
      Params.empty,
      bqfr"select 1 as n",
      BQSchema.of(
        BQField("n", BQField.Type.INT64, BQField.Mode.NULLABLE)
      )
    )

    RoutineUpdateOperation.from(tvf, Some(routine)) match {
      case _: UpdateOperation.Noop =>
      case other => fail(other.toString)
    }
  }

}
