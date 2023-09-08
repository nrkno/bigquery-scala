/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
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
  private val source2: BQTableDef.Table[LocalDate] =
    BQTableDef.Table(tableId("src_2"), BQSchema(List(c1)), BQPartitionType.DatePartitioned(c1.ident))

  test("using LocalDate partition fills") {
    val executionDate = LocalDate.now()
    val dest1: BQTableDef.Table[LocalDate] =
      BQTableDef.Table(tableId("dest_1"), BQSchema(List(c1)), BQPartitionType.DatePartitioned(c1.ident))
    val dest2: BQTableDef.Table[LocalDate] =
      BQTableDef.Table(tableId("dest_2"), BQSchema(List(c1)), BQPartitionType.DatePartitioned(c1.ident))

    val fillQuery1 = bqfr"select c1 from ${source1.assertPartition(executionDate)}"
    val bqFill: BQFill[LocalDate] = BQFill(JobKey("j1"), dest1, fillQuery1, executionDate)

    val fillQuery2 = bqfr"select c1 from ${source2.assertPartition(executionDate)}"
    val bqFilledTable: BQFilledTable[LocalDate] = BQFilledTable(JobKey("j2"), dest2, _ => fillQuery2)

    val query = bqfr"select c1 from $bqFill t1 join $bqFilledTable t2 on t1.${c1.ident} = t2${c1.ident}"
    val (fillTables: List[BQTableId], fillJobKeys: List[JobKeyBQ]) = extractTableIdAndJobKeys(query)

    assertEquals(fillJobKeys, List(JobKey("j1"), JobKey("j2")))
    assertEquals(fillTables, List(tableId("dest_1"), tableId("dest_2")))
  }

  test("using unpartition fills") {
    val dest1: BQTableDef.Table[Unit] =
      BQTableDef.Table(tableId("dest_1"), BQSchema(List(c1)), BQPartitionType.NotPartitioned)
    val dest2: BQTableDef.Table[Unit] =
      BQTableDef.Table(tableId("dest_2"), BQSchema(List(c1)), BQPartitionType.NotPartitioned)

    val fillQuery1 = bqfr"select c1 from ${source1.unpartitioned}"
    val bqFill: BQFill[Unit] = BQFill(JobKey("j1"), dest1, fillQuery1, ())

    val fillQuery2 = bqfr"select c1 from ${source2.unpartitioned}"
    val bqFilledTable: BQFilledTable[Unit] = BQFilledTable(JobKey("j2"), dest2, _ => fillQuery2)

    val query = bqfr"select c1 from $bqFill t1 join $bqFilledTable t2 on t1.${c1.ident} = t2${c1.ident}"
    val (fillTables: List[BQTableId], fillJobKeys: List[JobKeyBQ]) = extractTableIdAndJobKeys(query)

    assertEquals(fillJobKeys, List(JobKey("j1"), JobKey("j2")))
    assertEquals(fillTables, List(tableId("dest_1"), tableId("dest_2")))
  }

  test("using mixed partition types in fills") {
    val executionDate = LocalDate.now()
    val dest1: BQTableDef.Table[LocalDate] =
      BQTableDef.Table(tableId("dest_1"), BQSchema(List(c1)), BQPartitionType.DatePartitioned(c1.ident))
    val dest2: BQTableDef.Table[Unit] =
      BQTableDef.Table(tableId("dest_2"), BQSchema(List(c1)), BQPartitionType.NotPartitioned)

    val fillQuery1 = bqfr"select c1 from ${source1.assertPartition(executionDate)}"
    val bqFill: BQFill[LocalDate] = BQFill(JobKey("j1"), dest1, fillQuery1, executionDate)

    val fillQuery2 = bqfr"select c1 from ${source2.unpartitioned}"
    val bqFilledTable: BQFilledTable[Unit] = BQFilledTable(JobKey("j2"), dest2, _ => fillQuery2)

    val query = bqfr"select c1 from $bqFill t1 join $bqFilledTable t2 on t1.${c1.ident} = t2${c1.ident}"
    val (fillTables: List[BQTableId], fillJobKeys: List[JobKeyBQ]) = extractTableIdAndJobKeys(query)

    assertEquals(fillJobKeys, List(JobKey("j1"), JobKey("j2")))
    assertEquals(fillTables, List(tableId("dest_1"), tableId("dest_2")))
  }

  private def extractTableIdAndJobKeys(fragment: BQSqlFrag): (List[BQTableId], List[JobKeyBQ]) = {
    val fillTables = fragment
      .collect {
        case BQSqlFrag.FillRef(fill) => fill.tableDef.tableId
        case BQSqlFrag.FilledTableRef(fill) => fill.tableDef.tableId
      }
      .sortBy(_.asString)
    val fillJobKeys = fragment
      .collect {
        case BQSqlFrag.FillRef(fill) => fill.jobKey
        case BQSqlFrag.FilledTableRef(fill) => fill.jobKey
      }
      .sortBy(_.toString)
    (fillTables, fillJobKeys)
  }
}
