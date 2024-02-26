/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.metrics

import cats.effect.kernel.Outcome
import cats.effect.{Clock, Concurrent, Resource}
import cats.syntax.all.*
import com.google.cloud.bigquery.{Job, JobStatistics}
import no.nrk.bigquery.BQJobId

import scala.concurrent.TimeoutException

object BQMetrics {
  def apply[F[_]](
      ops: MetricsOps[F],
      jobId: BQJobId
  )(
      job: F[Option[Job]]
  )(implicit F: Clock[F], C: Concurrent[F]): F[Option[Job]] =
    effect(ops, jobId)(job)

  def effect[F[_]](ops: MetricsOps[F], jobId: BQJobId)(
      job: F[Option[Job]]
  )(implicit F: Clock[F], C: Concurrent[F]): F[Option[Job]] =
    withMetrics(job, ops, jobId)

  private def withMetrics[F[_]](
      job: F[Option[Job]],
      ops: MetricsOps[F],
      jobId: BQJobId
  )(implicit F: Clock[F], C: Concurrent[F]): F[Option[Job]] =
    (for {
      start <- Resource.eval(F.monotonic)
      resp <- executeRequestAndRecordMetrics(
        job,
        ops,
        jobId,
        start.toNanos
      )
    } yield resp).use(C.pure)

  private def executeRequestAndRecordMetrics[F[_]](
      job: F[Option[Job]],
      ops: MetricsOps[F],
      jobId: BQJobId,
      start: Long
  )(implicit F: Clock[F], C: Concurrent[F]): Resource[F, Option[Job]] =
    (for {
      _ <- Resource.make(ops.increaseActiveJobs(jobId))(_ => ops.decreaseActiveJobs(jobId))
      _ <- Resource.onFinalize(
        F.monotonic.flatMap(now => ops.recordTotalTime(now.toNanos - start, jobId))
      )
      jobResult <- Resource.eval(job)
      _ <- Resource.eval(
        ops.recordComplete(
          jobResult.map(_.getStatistics[JobStatistics]),
          jobId
        )
      )
    } yield jobResult)
      .guaranteeCase {
        case Outcome.Succeeded(fa) => fa.void
        case Outcome.Errored(e) =>
          Resource.eval(
            registerError(start, ops, jobId)(e) *> C.raiseError(e)
          )
        case Outcome.Canceled() =>
          Resource.eval(
            F.monotonic.flatMap(now =>
              ops
                .recordAbnormalTermination(
                  now.toNanos - start,
                  TerminationType.Canceled,
                  jobId
                ))
          )
      }

  private def registerError[F[_]](
      start: Long,
      ops: MetricsOps[F],
      jobId: BQJobId
  )(
      e: Throwable
  )(implicit F: Clock[F], C: Concurrent[F]): F[Unit] =
    F.monotonic
      .flatMap { now =>
        if (e.isInstanceOf[TimeoutException])
          ops.recordAbnormalTermination(
            now.toNanos - start,
            TerminationType.Timeout,
            jobId
          )
        else
          ops.recordAbnormalTermination(
            now.toNanos - start,
            TerminationType.Error(e),
            jobId
          )
      }
}
