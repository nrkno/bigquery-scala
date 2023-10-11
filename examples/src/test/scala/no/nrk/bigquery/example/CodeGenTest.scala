/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package example

class CodeGenTest extends munit.FunSuite with testing.GeneratedTest {

  test("generate code") {
    val generated = CodeGen.generate(
      List(
        BQTableDef.Table(
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
        )),
      List("example")
    )
    val head = generated.head
    val path = basedir.resolve("codegen-tests")
    writeAndCompare(head.loc.destinationFile(path), head.asObject)
  }

  override def testType: String = "codegen"
}
