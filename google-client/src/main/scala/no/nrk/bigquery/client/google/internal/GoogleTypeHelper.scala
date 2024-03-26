/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package client.google.internal

import com.google.cloud.bigquery.*
import com.google.cloud.bigquery.JobInfo.WriteDisposition as GoogleWriteDisposition
import no.nrk.bigquery.TableLabels.Empty

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.collection.immutable.SortedMap
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

object GoogleTypeHelper {

  def toDatasetGoogle(ds: BQDataset.Ref): DatasetId = DatasetId.of(ds.project.value, ds.id)
  def toTableIdGoogle(tableId: BQTableId): TableId =
    TableId.of(tableId.dataset.project.value, tableId.dataset.id, tableId.tableName)

  def toTableOptions(tableInfo: TableInfo): TableOptions = TableOptions(
    partitionFilterRequired = Option(tableInfo.getRequirePartitionFilter).exists(_.booleanValue()),
    tableInfo.getDefinition[TableDefinition] match {
      case definition: StandardTableDefinition =>
        Option(definition.getTimePartitioning)
          .flatMap(tp => Option(tp.getExpirationMs))
          .map(expires => FiniteDuration(expires, TimeUnit.MILLISECONDS))
      case _ => None
    }
  )

  def unsafeTableIdFromGoogle(dataset: BQDataset.Ref, tableId: TableId): BQTableId = {
    require(
      tableId.getProject == dataset.project.value && dataset.id == tableId.getDataset,
      s"Expected google table Id($tableId) to be the same datasetId and project as provided dataset[$dataset]"
    )
    BQTableId(dataset, tableId.getTable)
  }

  implicit class BQTableIdOps(val tableId: BQTableId) extends AnyVal {
    def underlying: TableId = toTableIdGoogle(tableId)
  }

  implicit class BQDatasetOps(val ds: BQDataset) extends AnyVal {
    def underlying: DatasetId = toDatasetGoogle(ds.toRef)
  }

  implicit class BQDatasetRefOps(val ds: BQDataset.Ref) extends AnyVal {
    def underlying: DatasetId = toDatasetGoogle(ds)
  }

  def tableLabelsfromTableInfo(tableInfo: TableInfo): TableLabels =
    Option(tableInfo.getLabels) match {
      case Some(values) => new TableLabels(SortedMap(values.asScala.toList*))
      case None => Empty
    }

  def jobIdFromJob(job: Job) =
    Option(job.getJobId)
      .map((ref: JobId) =>
        BQJobId(
          Option(ref.getProject).map(ProjectId.unsafeFromString),
          Option(ref.getLocation).map(LocationId.apply),
          Option(ref.getJob).getOrElse(""),
          JobLabels.unsafeFrom((job.getConfiguration[JobConfiguration].getType match {
            case JobConfiguration.Type.COPY =>
              Option(job.getConfiguration[CopyJobConfiguration].getLabels).map(_.asScala.toList)
            case JobConfiguration.Type.EXTRACT =>
              Option(job.getConfiguration[ExtractJobConfiguration].getLabels).map(_.asScala.toList)
            case JobConfiguration.Type.LOAD =>
              Option(job.getConfiguration[LoadJobConfiguration].getLabels).map(_.asScala.toList)
            case JobConfiguration.Type.QUERY =>
              Option(job.getConfiguration[QueryJobConfiguration].getLabels).map(_.asScala.toList)
          }).getOrElse(Nil)*)
        ))
      .get

  def toGoogleDisposition(writeDisposition: WriteDisposition) = writeDisposition match {
    case WriteDisposition.WRITE_TRUNCATE => GoogleWriteDisposition.WRITE_TRUNCATE
    case WriteDisposition.WRITE_APPEND => GoogleWriteDisposition.WRITE_APPEND
    case WriteDisposition.WRITE_EMPTY => GoogleWriteDisposition.WRITE_EMPTY
  }

  def toQueryStats(jobId: BQJobId, statistics: JobStatistics.QueryStatistics) =
    BQJobStatistics.Query(
      jobId,
      Option(statistics.getCreationTime).map(_.longValue()).map(Instant.ofEpochMilli),
      Option(statistics.getStartTime).map(_.longValue()).map(Instant.ofEpochMilli),
      Option(statistics.getEndTime).map(_.longValue()).map(Instant.ofEpochMilli),
      Option(statistics.getNumChildJobs).map(_.longValue()).getOrElse(0),
      Option(statistics.getParentJobId),
      Option(statistics.getSchema).map(SchemaHelper.fromSchema),
      Option(statistics.getBillingTier).map(_.intValue()),
      Option(statistics.getEstimatedBytesProcessed).map(_.longValue()),
      Option(statistics.getNumDmlAffectedRows).map(_.longValue()),
      Option(statistics.getTotalBytesBilled).map(_.longValue()),
      Option(statistics.getTotalBytesProcessed).map(_.longValue()),
      Option(statistics.getTotalPartitionsProcessed).map(_.longValue()),
      Option(statistics.getTotalSlotMs).map(_.longValue())
    )

  def toLoadStats(jobId: BQJobId, statistics: JobStatistics.LoadStatistics) =
    BQJobStatistics.Load(
      jobId,
      Option(statistics.getCreationTime).map(_.longValue()).map(Instant.ofEpochMilli),
      Option(statistics.getStartTime).map(_.longValue()).map(Instant.ofEpochMilli),
      Option(statistics.getEndTime).map(_.longValue()).map(Instant.ofEpochMilli),
      Option(statistics.getNumChildJobs).map(_.longValue()).getOrElse(0),
      Option(statistics.getParentJobId),
      Option(statistics.getInputBytes).map(_.longValue()),
      Option(statistics.getInputFiles).map(_.longValue()),
      Option(statistics.getOutputBytes).map(_.longValue()),
      Option(statistics.getOutputRows).map(_.longValue()),
      Option(statistics.getBadRecords).map(_.longValue())
    )

  def toExtractStats(jobId: BQJobId, statistics: JobStatistics.ExtractStatistics) =
    BQJobStatistics.Extract(
      jobId,
      Option(statistics.getCreationTime).map(_.longValue()).map(Instant.ofEpochMilli),
      Option(statistics.getStartTime).map(_.longValue()).map(Instant.ofEpochMilli),
      Option(statistics.getEndTime).map(_.longValue()).map(Instant.ofEpochMilli),
      Option(statistics.getNumChildJobs).map(_.longValue()).getOrElse(0),
      Option(statistics.getParentJobId),
      Option(statistics.getInputBytes).map(_.longValue()),
      Option(statistics.getDestinationUriFileCounts).map(_.asScala.map(_.longValue()).toList)
    )

  def toStats(jobId: BQJobId, statistics: JobStatistics): Option[BQJobStatistics] = statistics match {
    case statistics: JobStatistics.LoadStatistics => Some(toLoadStats(jobId, statistics))
    case statistics: JobStatistics.QueryStatistics => Some(toQueryStats(jobId, statistics))
    case statistics: JobStatistics.ExtractStatistics => Some(toExtractStats(jobId, statistics))
    case _ => None
  }
}
