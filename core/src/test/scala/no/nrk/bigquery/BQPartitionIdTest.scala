package no.nrk.bigquery

import com.google.cloud.bigquery.TableId
import no.nrk.bigquery.testing.BQSmokeTest
import no.nrk.bigquery.implicits._

import java.time.LocalDate

class BQPartitionIdTest extends BQSmokeTest {
  val partitionDate: Ident = Ident("partitionDate")
  // here because of a failing test since we deleted the table? sorry
  val PodcastRaw = BQTableRef(
    TableId.of("nrk-datahub", "raw_data", "podcast_access_logs_v02"),
    BQPartitionType.DatePartitioned(partitionDate)
  )
  val P3Article = BQTableRef(
    TableId.of("nrk-datahub", "prod", "p3_article"),
    BQPartitionType.NotPartitioned
  )
  val NrkBeta = BQTableRef(
    TableId.of("nrk-ga-export", "5672922", "ga_sessions"),
    BQPartitionType.Sharded
  )
  val NrkNo = BQTableRef(
    TableId.of("nrk-recommendations", "6666907", "ga_sessions"),
    BQPartitionType.Sharded
  )

  bqCheckFragmentNoSchemaTest("shards") {
    // should handle different tables. it won't necessarily work, but who are we to judge?
    val partitions = List(
      NrkBeta.assertPartition(LocalDate.of(2021, 1, 1)),
      NrkNo.assertPartition(LocalDate.of(2021, 1, 3)),
      NrkNo.assertPartition(LocalDate.of(2021, 1, 1))
    )
    bqsql"select count(*) from $partitions"
  }

  bqCheckFragmentTestFailing("utopic", errorFragment = "not") {
    val partitions = List(
      NrkNo.assertPartition(LocalDate.of(2021, 1, 1)),
      NrkNo.assertPartition(LocalDate.of(2021, 1, 3)),
      NrkNo.assertPartition(LocalDate.of(3021, 1, 3)),
      NrkNo.assertPartition(LocalDate.of(3021, 1, 1))
    )

    bqsql"select count(*) from $partitions"
  }

  bqCheckFragmentNoSchemaTest("shard") {
    val partition = List(NrkNo.assertPartition(LocalDate.of(2021, 1, 1)))
    bqsql"select count(*) from $partition"
  }

  bqCheckFragmentNoSchemaTest("date-partitions") {
    val partitions = List(
      PodcastRaw.assertPartition(LocalDate.of(2021, 1, 1)),
      PodcastRaw.assertPartition(LocalDate.of(2021, 1, 3))
    )
    bqsql"select count(*) from $partitions"
  }

  bqCheckFragmentNoSchemaTest("date-partition") {
    val partitions = List(
      PodcastRaw.assertPartition(LocalDate.of(2021, 1, 1))
    )
    bqsql"select count(*) from $partitions"
  }

  bqCheckFragmentNoSchemaTest("unpartitioned") {
    val partitions = List(
      P3Article.assertPartition,
      P3Article.assertPartition
    )
    bqsql"select count(*) from $partitions"
  }

  bqCheckLegacyFragmentTest("PartitionLoader.nonPartitionedQuery") {
    PartitionLoader.unpartitioned.partitionQuery(P3Article.tableId).sql
  }

  bqCheckLegacyFragmentTest("PartitionLoader.shardedQuery with startdate") {
    PartitionLoader.shard
      .allPartitionsQuery(StartDate.FromDate(LocalDate.of(2020, 1, 1)), NrkNo)
      .sql
  }

  bqCheckLegacyFragmentTest("PartitionLoader.shardedQuery without startdate") {
    PartitionLoader.shard.allPartitionsQuery(StartDate.All, NrkNo).sql
  }

  bqCheckLegacyFragmentTest(
    "PartitionLoader.date.allPartitionsQuery with startdate"
  ) {
    PartitionLoader.date
      .allPartitionsQuery(
        PodcastRaw,
        StartDate.FromDate(LocalDate.of(2020, 1, 1))
      )
      .sql
  }

  bqCheckLegacyFragmentTest(
    "PartitionLoader.date.allPartitionsQuery without startdate"
  ) {
    PartitionLoader.date.allPartitionsQuery(PodcastRaw, StartDate.All).sql
  }

  bqCheckTest("PartitionLoader.date.rowCountQuery with startdate") {
    PartitionLoader.date.rowCountQuery(
      PodcastRaw,
      partitionDate,
      StartDate.FromDate(LocalDate.of(2020, 1, 1))
    )
  }

  bqCheckTest("PartitionLoader.date.rowCountQuery without startdate") {
    PartitionLoader.date.rowCountQuery(PodcastRaw, partitionDate, StartDate.All)
  }
}
