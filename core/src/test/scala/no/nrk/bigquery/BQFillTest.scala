/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import munit.FunSuite
import no.nrk.bigquery.syntax._

import java.time.LocalDate

class BQFillTest extends FunSuite {
  case class JobKey(value: String) extends JobKeyBQ

  private def tableId(name: String) = BQTableId(BQDataset(ProjectId("p1"), "d1", None), name)

  private val c1 = BQField("c1", BQField.Type.DATE, BQField.Mode.REQUIRED)
  private val source1: BQTableDef.Table[LocalDate] =
    BQTableDef.Table(tableId("src_1"), BQSchema(List(c1)), BQPartitionType.DatePartitioned(c1.ident))

  test("using LocalDate partition fills") {
    val executionDate = LocalDate.now()
    val dest1: BQTableDef.Table[LocalDate] =
      BQTableDef.Table(tableId("dest_1"), BQSchema(List(c1)), BQPartitionType.DatePartitioned(c1.ident))

    val fillQuery1 = bqfr"select c1 from ${source1.assertPartition(executionDate)}"
    val bqFill: BQFill[LocalDate] = BQFill(JobKey("j1"), dest1, fillQuery1, executionDate)

    val query = bqfr"select c1 from $bqFill t1"
    val (fillTables: List[BQTableId], fillJobKeys: List[JobKeyBQ]) = extractTableIdAndJobKeys(query)

    assertEquals(fillJobKeys, List(JobKey("j1")))
    assertEquals(fillTables, List(tableId("dest_1")))
  }

  test("using unpartition fills") {
    val dest1: BQTableDef.Table[Unit] =
      BQTableDef.Table(tableId("dest_1"), BQSchema(List(c1)), BQPartitionType.NotPartitioned)

    val fillQuery1 = bqfr"select c1 from ${source1.unpartitioned}"
    val bqFill: BQFill[Unit] = BQFill(JobKey("j1"), dest1, fillQuery1, ())

    val query = bqfr"select c1 from $bqFill t1"
    val (fillTables: List[BQTableId], fillJobKeys: List[JobKeyBQ]) = extractTableIdAndJobKeys(query)

    assertEquals(fillJobKeys, List(JobKey("j1")))
    assertEquals(fillTables, List(tableId("dest_1")))
  }

  test("using mixed partition types in fills") {
    val executionDate = LocalDate.now()
    val dest1: BQTableDef.Table[LocalDate] =
      BQTableDef.Table(tableId("dest_1"), BQSchema(List(c1)), BQPartitionType.DatePartitioned(c1.ident))

    val fillQuery1 = bqfr"select c1 from ${source1.assertPartition(executionDate)}"
    val bqFill: BQFill[LocalDate] = BQFill(JobKey("j1"), dest1, fillQuery1, executionDate)

    val query = bqfr"select c1 from $bqFill t1"
    val (fillTables: List[BQTableId], fillJobKeys: List[JobKeyBQ]) = extractTableIdAndJobKeys(query)

    assertEquals(fillJobKeys, List(JobKey("j1")))
    assertEquals(fillTables, List(tableId("dest_1")))
  }

  private def extractTableIdAndJobKeys(fragment: BQSqlFrag): (List[BQTableId], List[JobKeyBQ]) = {
    val fillTables = fragment
      .collect { case BQSqlFrag.FillRef(fill) =>
        fill.tableDef.tableId
      }
      .sortBy(_.asString)
    val fillJobKeys = fragment
      .collect { case BQSqlFrag.FillRef(fill) =>
        fill.jobKey
      }
      .sortBy(_.toString)
    (fillTables, fillJobKeys)
  }
}
