/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package no.nrk.bigquery.internal

import com.google.cloud.bigquery.{RoutineId, RoutineInfo}
import munit.FunSuite
import no.nrk.bigquery.UpdateOperation.{CreatePersistentUdf, Illegal, UpdatePersistentUdf}
import no.nrk.bigquery._
import no.nrk.bigquery.syntax._
import no.nrk.bigquery.internal.nat._0

class UdfUpdateOperationTest extends FunSuite {

  private val udf: UDF.Persistent[_0] =
    UDF.persistent(
      ident"foo",
      BQDataset(ProjectId("p1"), "ds1", None),
      UDF.Params.empty,
      UDF.Body.Sql(bqfr"(1)"),
      Some(BQType.INT64))
  private val routineId: RoutineId = RoutineId.of("p1", "ds1", "foo")

  test("should create when it does not exist") {
    UdfUpdateOperation.from(udf, None) match {
      case CreatePersistentUdf(_, routine) =>
        assertEquals(routine.getRoutineId, routineId)
      case other => fail(other.toString)
    }
  }

  test("should update udf when changed") {
    val existingRoutine = UdfUpdateOperation.from(udf, None) match {
      case CreatePersistentUdf(_, routine) => routine
      case other => fail(s"test setup failed: ${other.toString}")
    }

    UdfUpdateOperation.from(
      udf.copy(params = UDF.Params.of(UDF.Param(ident"foo", Some(BQType.INT64)))),
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

    UdfUpdateOperation.from(udf, Some(routine)) match {
      case Illegal(_, _) =>
      case other => fail(other.toString)
    }
  }

}
