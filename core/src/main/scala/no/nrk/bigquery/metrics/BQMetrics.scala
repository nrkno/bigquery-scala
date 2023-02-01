package no.nrk.bigquery.metrics

import cats.effect.kernel.Outcome
import cats.effect.{Clock, Concurrent, Resource}
import cats.syntax.all._
import com.google.cloud.bigquery.{Job, JobStatistics}
import no.nrk.bigquery.BQJobName

import scala.concurrent.TimeoutException

object BQMetrics {
  def apply[F[_]](
      ops: MetricsOps[F],
      jobName: BQJobName
  )(
      job: F[Option[Job]]
  )(implicit F: Clock[F], C: Concurrent[F]): F[Option[Job]] =
    effect(ops, jobName)(job)

  def effect[F[_]](ops: MetricsOps[F], jobName: BQJobName)(
      job: F[Option[Job]]
  )(implicit F: Clock[F], C: Concurrent[F]): F[Option[Job]] =
    withMetrics(job, ops, jobName)

  private def withMetrics[F[_]](
      job: F[Option[Job]],
      ops: MetricsOps[F],
      jobName: BQJobName
  )(implicit F: Clock[F], C: Concurrent[F]): F[Option[Job]] =
    (for {
      start <- Resource.eval(F.monotonic)
      resp <- executeRequestAndRecordMetrics(
        job,
        ops,
        jobName,
        start.toNanos
      )
    } yield resp).use(C.pure)

  private def executeRequestAndRecordMetrics[F[_]](
      job: F[Option[Job]],
      ops: MetricsOps[F],
      jobName: BQJobName,
      start: Long
  )(implicit F: Clock[F], C: Concurrent[F]): Resource[F, Option[Job]] =
    (for {
      _ <- Resource.make(ops.increaseActiveRequests(jobName))(_ =>
        ops.decreaseActiveRequests(jobName)
      )
      _ <- Resource.onFinalize(
        F.monotonic.flatMap(now =>
          ops.recordTotalTime(now.toNanos - start, jobName)
        )
      )
      jobResult <- Resource.eval(job)
      _ <- Resource.eval(
        ops.recordTotalBytesBilled(
          jobResult.map(_.getStatistics[JobStatistics]),
          jobName
        )
      )
    } yield jobResult)
      .guaranteeCase {
        case Outcome.Succeeded(fa) => fa.void
        case Outcome.Errored(e) =>
          Resource.eval(
            registerError(start, ops, jobName)(e) *> C.raiseError(e)
          )
        case Outcome.Canceled() =>
          Resource.eval(
            F.monotonic.flatMap(now =>
              ops
                .recordAbnormalTermination(
                  now.toNanos - start,
                  TerminationType.Canceled,
                  jobName
                )
            )
          )
      }

  private def registerError[F[_]](
      start: Long,
      ops: MetricsOps[F],
      jobName: BQJobName
  )(
      e: Throwable
  )(implicit F: Clock[F], C: Concurrent[F]): F[Unit] =
    F.monotonic
      .flatMap { now =>
        if (e.isInstanceOf[TimeoutException])
          ops.recordAbnormalTermination(
            now.toNanos - start,
            TerminationType.Timeout,
            jobName
          )
        else
          ops.recordAbnormalTermination(
            now.toNanos - start,
            TerminationType.Error(e),
            jobName
          )
      }
}
