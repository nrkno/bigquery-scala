/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.metrics

import cats.Applicative
import no.nrk.bigquery.{BQJobId, BQJobStatistics}

trait MetricsOps[F[_]] {
  def increaseActiveJobs(jobId: BQJobId): F[Unit]
  def decreaseActiveJobs(jobId: BQJobId): F[Unit]
  def recordTotalTime(elapsed: Long, jobId: BQJobId): F[Unit]
  def recordAbnormalTermination(
      elapsed: Long,
      terminationType: TerminationType,
      jobId: BQJobId
  ): F[Unit]
  def recordComplete(
      jobStats: Option[BQJobStatistics],
      jobId: BQJobId
  ): F[Unit]
}

object MetricsOps {
  def noop[F[_]](implicit F: Applicative[F]): MetricsOps[F] = new MetricsOps[F] {
    override def increaseActiveJobs(jobId: BQJobId): F[Unit] =
      F.unit

    override def decreaseActiveJobs(jobId: BQJobId): F[Unit] =
      F.unit

    override def recordTotalTime(
        elapsed: Long,
        jobId: BQJobId
    ): F[Unit] = F.unit

    override def recordAbnormalTermination(
        elapsed: Long,
        terminationType: TerminationType,
        jobId: BQJobId
    ): F[Unit] = F.unit

    override def recordComplete(
        jobStats: Option[BQJobStatistics],
        jobId: BQJobId
    ): F[Unit] = F.unit
  }
}

/** Describes the type of abnormal termination */

sealed trait TerminationType
object TerminationType {

  /** Signals just a generic abnormal termination */
  case class Abnormal(rootCause: Throwable) extends TerminationType

  /** Signals cancelation */
  case object Canceled extends TerminationType

  /** Signals an abnormal termination due to an error processing the request, either at the server or client side
    */
  case class Error(rootCause: Throwable) extends TerminationType

  /** Signals a client timing out during a request */
  case object Timeout extends TerminationType

}
