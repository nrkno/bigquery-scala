/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package metrics

import cats.data.NonEmptyList
import cats.effect.{Resource, Sync}
import cats.syntax.apply._
import com.google.cloud.bigquery.JobStatistics
import com.google.cloud.bigquery.JobStatistics.QueryStatistics
import io.prometheus.client._

object Prometheus {
  def collectorRegistry[F[_]](implicit
      F: Sync[F]
  ): Resource[F, CollectorRegistry] =
    Resource.make(F.delay(new CollectorRegistry()))(cr => F.blocking(cr.clear()))

  /** Creates a [[MetricsOps]] that supports Prometheus metrics
    *
    * @param registry
    *   a metrics collector registry
    * @param prefix
    *   a prefix that will be added to all metrics
    */
  object DefaultMetricsOps {

    def apply[F[_]: Sync](
        registry: CollectorRegistry
    ): Resource[F, MetricsOps[F]] =
      apply(registry, "com_google_bigquery")

    def apply[F[_]: Sync](
        registry: CollectorRegistry,
        prefix: String
    ): Resource[F, MetricsOps[F]] =
      apply(registry, prefix, job => Some(job.name))

    def apply[F[_]: Sync](
        registry: CollectorRegistry,
        prefix: String,
        classifierF: BQJobId => Option[String]
    ): Resource[F, MetricsOps[F]] =
      apply(registry, prefix, classifierF, DefaultHistogramBuckets)

    def apply[F[_]: Sync](
        registry: CollectorRegistry,
        prefix: String,
        classifierF: BQJobId => Option[String],
        responseDurationSecondsHistogramBuckets: NonEmptyList[Double]
    ): Resource[F, MetricsOps[F]] =
      for {
        metrics <- createMetricsCollection(
          registry,
          prefix,
          responseDurationSecondsHistogramBuckets
        )
      } yield createMetricsOps(metrics, classifierF)

    private def createMetricsOps[F[_]](
        metrics: MetricsCollection,
        classifierF: BQJobId => Option[String]
    )(implicit F: Sync[F]): MetricsOps[F] =
      new MetricsOps[F] {
        override def increaseActiveJobs(
            jobId: BQJobId
        ): F[Unit] =
          F.delay {
            metrics.activeJobs
              .labels(label(classifierF(jobId)))
              .inc()
          }

        override def decreaseActiveJobs(
            jobId: BQJobId
        ): F[Unit] =
          F.delay {
            metrics.activeJobs
              .labels(label(classifierF(jobId)))
              .dec()
          }

        override def recordTotalTime(
            elapsed: Long,
            jobId: BQJobId
        ): F[Unit] =
          F.delay {
            metrics.jobDuration
              .labels(label(classifierF(jobId)))
              .observe(SimpleTimer.elapsedSecondsFromNanos(0, elapsed))
            metrics.jobs
              .labels(label(classifierF(jobId)))
              .inc()
          }

        override def recordAbnormalTermination(
            elapsed: Long,
            terminationType: TerminationType,
            jobId: BQJobId
        ): F[Unit] =
          terminationType match {
            case TerminationType.Abnormal(e) =>
              recordAbnormal(elapsed, jobId, e)
            case TerminationType.Error(e) => recordError(elapsed, jobId, e)
            case TerminationType.Canceled => recordCanceled(elapsed, jobId)
            case TerminationType.Timeout => recordTimeout(elapsed, jobId)
          }

        private def recordCanceled(
            elapsed: Long,
            jobName: BQJobId
        ): F[Unit] =
          F.delay {
            metrics.abnormalTerminations
              .labels(
                label(classifierF(jobName)),
                AbnormalTermination.report(AbnormalTermination.Canceled),
                label(Option.empty)
              )
              .observe(SimpleTimer.elapsedSecondsFromNanos(0, elapsed))
          }

        private def recordAbnormal(
            elapsed: Long,
            jobName: BQJobId,
            cause: Throwable
        ): F[Unit] =
          F.delay {
            metrics.abnormalTerminations
              .labels(
                label(classifierF(jobName)),
                AbnormalTermination.report(AbnormalTermination.Abnormal),
                label(Option(cause.getClass.getName))
              )
              .observe(SimpleTimer.elapsedSecondsFromNanos(0, elapsed))
          }

        private def recordError(
            elapsed: Long,
            jobName: BQJobId,
            cause: Throwable
        ): F[Unit] =
          F.delay {
            metrics.abnormalTerminations
              .labels(
                label(classifierF(jobName)),
                AbnormalTermination.report(AbnormalTermination.Error),
                label(Option(cause.getClass.getName))
              )
              .observe(SimpleTimer.elapsedSecondsFromNanos(0, elapsed))
          }

        private def recordTimeout(
            elapsed: Long,
            jobName: BQJobId
        ): F[Unit] =
          F.delay {
            metrics.abnormalTerminations
              .labels(
                label(classifierF(jobName)),
                AbnormalTermination.report(AbnormalTermination.Timeout),
                label(Option.empty)
              )
              .observe(SimpleTimer.elapsedSecondsFromNanos(0, elapsed))
          }

        override def recordComplete(
            jobStats: Option[JobStatistics],
            jobId: BQJobId
        ): F[Unit] =
          jobStats
            .collect { case stats: QueryStatistics =>
              Option(stats.getTotalBytesBilled) -> Option(stats.getTotalSlotMs)
            }
            .map { case (maybeBilled, maybeTotalSlotsMs) =>
              F.delay {
                maybeBilled.foreach(totalBytes =>
                  metrics.bytesBilled
                    .labels(
                      label(classifierF(jobId))
                    )
                    .inc(totalBytes.toDouble))

                maybeTotalSlotsMs.foreach(total =>
                  metrics.totalSlotsMs
                    .labels(
                      label(classifierF(jobId))
                    )
                    .inc(total.toDouble))
              }
            }
            .getOrElse(F.unit)

        private def label(value: Option[String]): String = value.getOrElse("")
      }

    private def createMetricsCollection[F[_]: Sync](
        registry: CollectorRegistry,
        prefix: String,
        jobDurationSecondsHistogramBuckets: NonEmptyList[Double]
    ): Resource[F, MetricsCollection] = {
      val jobDuration: Resource[F, Histogram] = registerCollector(
        Histogram
          .build()
          .buckets(jobDurationSecondsHistogramBuckets.toList: _*)
          .name(prefix + "_" + "job_duration_seconds")
          .help("Job Duration in seconds.")
          .labelNames("classifier")
          .create(),
        registry
      )

      val activeJobs: Resource[F, Gauge] = registerCollector(
        Gauge
          .build()
          .name(prefix + "_" + "active_jobs_count")
          .help("Total Active Jobs Requests.")
          .labelNames("classifier")
          .create(),
        registry
      )

      val jobs: Resource[F, Counter] = registerCollector(
        Counter
          .build()
          .name(prefix + "_" + "jobs_count")
          .help("Total Jobs.")
          .labelNames("classifier")
          .create(),
        registry
      )

      val abnormalTerminations: Resource[F, Histogram] = registerCollector(
        Histogram
          .build()
          .name(prefix + "_" + "abnormal_terminations")
          .help("Total Abnormal Terminations.")
          .labelNames("classifier", "termination_type", "cause")
          .create(),
        registry
      )

      val bytesBilled: Resource[F, Counter] = registerCollector(
        Counter
          .build()
          .name(prefix + "_" + "bytes_billed")
          .help("Total bytes billed.")
          .labelNames("classifier")
          .create(),
        registry
      )
      val totalSlotMs: Resource[F, Counter] = registerCollector(
        Counter
          .build()
          .name(prefix + "_" + "total_slot_ms")
          .help("Total Slot milliseconds.")
          .labelNames("classifier")
          .create(),
        registry
      )

      (
        jobDuration,
        activeJobs,
        jobs,
        abnormalTerminations,
        bytesBilled,
        totalSlotMs
      ).mapN(MetricsCollection.apply)
    }
  }

  private def registerCollector[F[_], C <: Collector](
      collector: C,
      registry: CollectorRegistry
  )(implicit F: Sync[F]): Resource[F, C] =
    Resource.make(F.blocking(collector.register[C](registry)))(c => F.blocking(registry.unregister(c)))

  // https://github.com/prometheus/client_java/blob/parent-0.6.0/simpleclient/src/main/java/io/prometheus/client/Histogram.java#L73
  private val DefaultHistogramBuckets: NonEmptyList[Double] =
    NonEmptyList(
      .005,
      List(.01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10)
    )
}

final case class MetricsCollection(
    jobDuration: Histogram,
    activeJobs: Gauge,
    jobs: Counter,
    abnormalTerminations: Histogram,
    bytesBilled: Counter,
    totalSlotsMs: Counter
)

private sealed trait AbnormalTermination
private object AbnormalTermination {
  case object Abnormal extends AbnormalTermination
  case object Error extends AbnormalTermination
  case object Timeout extends AbnormalTermination
  case object Canceled extends AbnormalTermination
  def report(t: AbnormalTermination): String =
    t match {
      case Abnormal => "abnormal"
      case Timeout => "timeout"
      case Error => "error"
      case Canceled => "cancel"
    }
}
