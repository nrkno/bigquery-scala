/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package client.http4s
package internal

import googleapis.bigquery.{Job, JobStatistics}

import java.time.Instant

object JobHelper {
  def jobId(job: Job) =
    job.jobReference
      .map(ref =>
        BQJobId(
          ref.projectId.map(ProjectId.unsafeFromString),
          ref.location.map(LocationId.apply),
          ref.jobId.getOrElse(""),
          JobLabels.unsafeFrom(job.configuration.flatMap(_.labels).getOrElse(Map.empty).toList*)
        ))

  def toStats(jobId: BQJobId, stat: JobStatistics): Option[BQJobStatistics] =
    toQueryStats(jobId, stat).orElse(toLoadStats(jobId, stat))

  def toQueryStats(jobId: BQJobId, stat: JobStatistics) =
    stat.query.map(qstat =>
      BQJobStatistics.Query(
        jobId,
        stat.creationTime.map(dur => Instant.ofEpochMilli(dur.toMillis)),
        stat.startTime.map(dur => Instant.ofEpochMilli(dur.toMillis)),
        stat.endTime.map(dur => Instant.ofEpochMilli(dur.toMillis)),
        stat.numChildJobs.getOrElse(0L),
        stat.parentJobId,
        qstat.schema.map(SchemaHelper.fromTableSchema),
        qstat.billingTier,
        qstat.estimatedBytesProcessed,
        qstat.numDmlAffectedRows,
        qstat.totalBytesBilled,
        qstat.totalBytesProcessed,
        qstat.totalPartitionsProcessed,
        qstat.totalSlotMs.map(_.toMillis)
      ))

  def toLoadStats(jobId: BQJobId, stat: JobStatistics) =
    stat.load.map(lstat =>
      BQJobStatistics.Load(
        jobId,
        stat.creationTime.map(dur => Instant.ofEpochMilli(dur.toMillis)),
        stat.startTime.map(dur => Instant.ofEpochMilli(dur.toMillis)),
        stat.endTime.map(dur => Instant.ofEpochMilli(dur.toMillis)),
        stat.numChildJobs.getOrElse(0L),
        stat.parentJobId,
        lstat.inputFileBytes,
        lstat.inputFiles,
        lstat.outputBytes,
        lstat.outputRows,
        lstat.badRecords
      ))

  def toExtractStats(jobId: BQJobId, stat: JobStatistics) =
    stat.extract.map(extract =>
      BQJobStatistics.Extract(
        jobId,
        stat.creationTime.map(dur => Instant.ofEpochMilli(dur.toMillis)),
        stat.startTime.map(dur => Instant.ofEpochMilli(dur.toMillis)),
        stat.endTime.map(dur => Instant.ofEpochMilli(dur.toMillis)),
        stat.numChildJobs.getOrElse(0L),
        stat.parentJobId,
        extract.inputBytes,
        extract.destinationUriFileCounts
      ))
}
