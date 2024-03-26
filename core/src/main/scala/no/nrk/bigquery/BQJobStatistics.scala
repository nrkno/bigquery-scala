/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import java.time.Instant

final case class JobWithStats[Job](job: Job, statistics: BQJobStatistics)

sealed trait BQJobStatistics {
  def jobId: BQJobId
  def creationTime: Option[Instant]
  def startTime: Option[Instant]
  def endTime: Option[Instant]
  def numChildJobs: Long
  def parentJobId: Option[String]
}

object BQJobStatistics {
  final case class Query(
      jobId: BQJobId,
      creationTime: Option[Instant],
      startTime: Option[Instant],
      endTime: Option[Instant],
      numChildJobs: Long,
      parentJobId: Option[String],
      // query stats selected fields
      schema: Option[BQSchema],
      billingTier: Option[Int],
      estimatedBytesProcessed: Option[Long],
      numDmlAffectedRows: Option[Long],
      totalBytesBilled: Option[Long],
      totalBytesProcessed: Option[Long],
      totalPartitionsProcessed: Option[Long],
      totalSlotMs: Option[Long]
  ) extends BQJobStatistics

  final case class Load(
      jobId: BQJobId,
      creationTime: Option[Instant],
      startTime: Option[Instant],
      endTime: Option[Instant],
      numChildJobs: Long,
      parentJobId: Option[String],
      // load stats
      inputBytes: Option[Long],
      inputFiles: Option[Long],
      outputBytes: Option[Long],
      outputRows: Option[Long],
      badRecords: Option[Long]
  ) extends BQJobStatistics

  // Extract
  final case class Extract(
      jobId: BQJobId,
      creationTime: Option[Instant],
      startTime: Option[Instant],
      endTime: Option[Instant],
      numChildJobs: Long,
      parentJobId: Option[String],
      // extract stats
      inputBytes: Option[Long],
      destinationUriFileCounts: Option[List[Long]]
  ) extends BQJobStatistics
  // Copy
}
