/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import no.nrk.bigquery.syntax.*
import no.nrk.bigquery.testing.{BQSmokeTest, BigQueryTestClient}
import no.nrk.bigquery.util.*

class TvfSmokeTest extends BQSmokeTest(BigQueryTestClient.testClient) {

  private val schema: BQSchema = BQSchema.of(
    BQField("id", BQField.Type.STRING, BQField.Mode.NULLABLE)
  )
  private val myDataset: BQDataset.Ref = BQDataset(ProjectId.unsafeFromString("some-project"), "my-dataset", None).toRef
  private val sourceTable = BQTableDef.Table(
    BQTableId.unsafeOf(myDataset, "source-table"),
    schema,
    BQPartitionType.NotPartitioned
  )
  val tvf: TVF[Unit, nat._1] = TVF(
    TVF.TVFId(myDataset, ident"my_tvf"),
    BQPartitionType.NotPartitioned,
    BQRoutine.Params(BQRoutine.Param("myId", BQType.STRING)),
    bqfr"select id from $sourceTable where id = myId",
    schema,
    None
  )

  bqCheckTableValueFunction("using_tvf", tvf)(args => args(StringValue("1")))

}
