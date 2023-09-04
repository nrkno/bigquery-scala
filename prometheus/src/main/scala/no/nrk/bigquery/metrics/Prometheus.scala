/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
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
      apply(registry, prefix, job => Some(job.value))

    def apply[F[_]: Sync](
        registry: CollectorRegistry,
        prefix: String,
        classifierF: BQJobName => Option[String]
    ): Resource[F, MetricsOps[F]] =
      apply(registry, prefix, classifierF, DefaultHistogramBuckets)

    def apply[F[_]: Sync](
        registry: CollectorRegistry,
        prefix: String,
        classifierF: BQJobName => Option[String],
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
        classifierF: BQJobName => Option[String]
    )(implicit F: Sync[F]): MetricsOps[F] =
      new MetricsOps[F] {
        override def increaseActiveJobs(
            jobName: BQJobName
        ): F[Unit] =
          F.delay {
            metrics.activeJobs
              .labels(label(classifierF(jobName)))
              .inc()
          }

        override def decreaseActiveJobs(
            jobName: BQJobName
        ): F[Unit] =
          F.delay {
            metrics.activeJobs
              .labels(label(classifierF(jobName)))
              .dec()
          }

        override def recordTotalTime(
            elapsed: Long,
            jobName: BQJobName
        ): F[Unit] =
          F.delay {
            metrics.jobDuration
              .labels(label(classifierF(jobName)))
              .observe(SimpleTimer.elapsedSecondsFromNanos(0, elapsed))
            metrics.jobs
              .labels(label(classifierF(jobName)))
              .inc()
          }

        override def recordAbnormalTermination(
            elapsed: Long,
            terminationType: TerminationType,
            jobName: BQJobName
        ): F[Unit] =
          terminationType match {
            case TerminationType.Abnormal(e) =>
              recordAbnormal(elapsed, jobName, e)
            case TerminationType.Error(e) => recordError(elapsed, jobName, e)
            case TerminationType.Canceled => recordCanceled(elapsed, jobName)
            case TerminationType.Timeout => recordTimeout(elapsed, jobName)
          }

        private def recordCanceled(
            elapsed: Long,
            jobName: BQJobName
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
            jobName: BQJobName,
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
            jobName: BQJobName,
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
            jobName: BQJobName
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

        override def recordTotalBytesBilled(
            job: Option[JobStatistics],
            jobName: BQJobName
        ): F[Unit] =
          job
            .collect { case stats: QueryStatistics =>
              stats.getTotalBytesBilled
            }
            .map(totalBytesBilled =>
              F.delay {
                metrics.bytesBilled
                  .labels(
                    label(classifierF(jobName))
                  )
                  .inc(totalBytesBilled.toDouble)
              })
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

      (
        jobDuration,
        activeJobs,
        jobs,
        abnormalTerminations,
        bytesBilled
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
    bytesBilled: Counter
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
