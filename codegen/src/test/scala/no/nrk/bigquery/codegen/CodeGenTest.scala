/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package codegen

import no.nrk.bigquery.syntax.*

class CodeGenTest extends munit.FunSuite with testing.GeneratedTest {
  val table = BQTableDef.Table(
    BQTableId.unsafeFromString("com-example.test.something-important"),
    BQSchema.of(
      BQField("partitionDate", BQField.Type.DATE, BQField.Mode.REQUIRED),
      BQField("name", BQField.Type.STRING, BQField.Mode.REQUIRED),
      BQField.repeated("list", BQField.Type.INT64),
      BQField.struct("complex", BQField.Mode.NULLABLE)(
        BQField("one", BQField.Type.STRING, BQField.Mode.REQUIRED),
        BQField("two", BQField.Type.STRING, BQField.Mode.REQUIRED),
        BQField("three", BQField.Type.BOOL, BQField.Mode.NULLABLE)
      )
    ),
    partitionType = BQPartitionType.DatePartitioned(Ident("partitionDate")),
    description = Some("desc"),
    clustering = Nil,
    labels = TableLabels.apply("foo" -> "bar"),
    TableOptions.Empty
  )

  test("generate table code") {
    val generated = CodeGen.generate(
      List(table),
      List("example")
    )
    val head = generated.head
    val path = basedir.resolve("codegen-tests")
    writeAndCompare(head.loc.destinationFile(path), head.asObject)
  }

  test("generate table integer range code") {
    val table = BQTableDef.Table(
      BQTableId.unsafeFromString("com-example.test.something-ranged"),
      BQSchema.of(
        BQField("id", BQField.Type.STRING, BQField.Mode.REQUIRED),
        BQField("name", BQField.Type.STRING, BQField.Mode.REQUIRED)
      ),
      partitionType = BQPartitionType.IntegerRangePartitioned(Ident("id")),
      description = Some("desc"),
      clustering = Nil,
      labels = TableLabels.apply("foo" -> "bar"),
      TableOptions.Empty
    )

    val generated = CodeGen.generate(
      List(table),
      List("example")
    )
    val head = generated.head
    val path = basedir.resolve("codegen-tests")
    writeAndCompare(head.loc.destinationFile(path), head.asObject)
  }

  test("generate view code") {
    val generated = CodeGen.generate(
      List(
        BQTableDef.View(
          BQTableId.unsafeFromString("com-example.test.something-important-view"),
          schema = BQSchema
            .of(
              table.schema.fields.head,
              BQField("count", BQField.Type.INT64, BQField.Mode.NULLABLE)
            )
            .recursivelyNullable,
          partitionType = BQPartitionType.DatePartitioned(Ident("partitionDate")),
          description = Some("desc"),
          query = bqsql"select partitionDate, COUNT(*) from ${table.unpartitioned} group by 1",
          labels = TableLabels.apply("foo" -> "bar")
        )),
      List("example")
    )
    val head = generated.head
    val path = basedir.resolve("codegen-tests")
    writeAndCompare(head.loc.destinationFile(path), head.asObject)
  }

  override def testType: String = "codegen"
}
