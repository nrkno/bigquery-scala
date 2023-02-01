import cats.data.NonEmptyList
import cats.effect.{Resource, Sync}
import cats.syntax.apply._
import com.google.cloud.bigquery.JobStatistics
import com.google.cloud.bigquery.JobStatistics.QueryStatistics
import io.prometheus.client._
import no.nrk.bigquery.metrics.{MetricsOps, TerminationType}

object Prometheus {
  def collectorRegistry[F[_]](implicit
      F: Sync[F]
  ): Resource[F, CollectorRegistry] =
    Resource.make(F.delay(new CollectorRegistry()))(cr =>
      F.blocking(cr.clear())
    )

  def NoopMetricsOps[F[_]](implicit F: Sync[F]): Resource[F, MetricsOps[F]] =
    Resource.pure(new MetricsOps[F] {
      override def increaseActiveRequests(classifier: Option[String]): F[Unit] =
        F.unit
      override def decreaseActiveRequests(classifier: Option[String]): F[Unit] =
        F.unit
      override def recordTotalTime(
          elapsed: Long,
          classifier: Option[String]
      ): F[Unit] = F.unit
      override def recordAbnormalTermination(
          elapsed: Long,
          terminationType: TerminationType,
          classifier: Option[String]
      ): F[Unit] = F.unit
      override def recordTotalBytesBilled(
          job: Option[JobStatistics],
          classifier: Option[String]
      ): F[Unit] = F.unit
    })

  /** Creates a [[MetricsOps]] that supports Prometheus metrics
    *
    * @param registry
    *   a metrics collector registry
    * @param prefix
    *   a prefix that will be added to all metrics
    */
  object DefaultMetricsOps {
    def apply[F[_]: Sync](
        registry: CollectorRegistry,
        prefix: String = "com_google_bigquery",
        responseDurationSecondsHistogramBuckets: NonEmptyList[Double] =
          DefaultHistogramBuckets
    ): Resource[F, MetricsOps[F]] =
      for {
        metrics <- createMetricsCollection(
          registry,
          prefix,
          responseDurationSecondsHistogramBuckets
        )
      } yield createMetricsOps(metrics)

    private def createMetricsOps[F[_]](
        metrics: MetricsCollection
    )(implicit F: Sync[F]): MetricsOps[F] =
      new MetricsOps[F] {
        override def increaseActiveRequests(
            classifier: Option[String]
        ): F[Unit] =
          F.delay {
            metrics.activeRequests
              .labels(label(classifier))
              .inc()
          }

        override def decreaseActiveRequests(
            classifier: Option[String]
        ): F[Unit] =
          F.delay {
            metrics.activeRequests
              .labels(label(classifier))
              .dec()
          }

        override def recordTotalTime(
            elapsed: Long,
            classifier: Option[String]
        ): F[Unit] =
          F.delay {
            metrics.responseDuration
              .labels(label(classifier))
              .observe(SimpleTimer.elapsedSecondsFromNanos(0, elapsed))
            metrics.requests
              .labels(label(classifier))
              .inc()
          }

        override def recordAbnormalTermination(
            elapsed: Long,
            terminationType: TerminationType,
            classifier: Option[String]
        ): F[Unit] =
          terminationType match {
            case TerminationType.Abnormal(e) =>
              recordAbnormal(elapsed, classifier, e)
            case TerminationType.Error(e) => recordError(elapsed, classifier, e)
            case TerminationType.Canceled => recordCanceled(elapsed, classifier)
            case TerminationType.Timeout  => recordTimeout(elapsed, classifier)
          }

        private def recordCanceled(
            elapsed: Long,
            classifier: Option[String]
        ): F[Unit] =
          F.delay {
            metrics.abnormalTerminations
              .labels(
                label(classifier),
                AbnormalTermination.report(AbnormalTermination.Canceled),
                label(Option.empty)
              )
              .observe(SimpleTimer.elapsedSecondsFromNanos(0, elapsed))
          }

        private def recordAbnormal(
            elapsed: Long,
            classifier: Option[String],
            cause: Throwable
        ): F[Unit] =
          F.delay {
            metrics.abnormalTerminations
              .labels(
                label(classifier),
                AbnormalTermination.report(AbnormalTermination.Abnormal),
                label(Option(cause.getClass.getName))
              )
              .observe(SimpleTimer.elapsedSecondsFromNanos(0, elapsed))
          }

        private def recordError(
            elapsed: Long,
            classifier: Option[String],
            cause: Throwable
        ): F[Unit] =
          F.delay {
            metrics.abnormalTerminations
              .labels(
                label(classifier),
                AbnormalTermination.report(AbnormalTermination.Error),
                label(Option(cause.getClass.getName))
              )
              .observe(SimpleTimer.elapsedSecondsFromNanos(0, elapsed))
          }

        private def recordTimeout(
            elapsed: Long,
            classifier: Option[String]
        ): F[Unit] =
          F.delay {
            metrics.abnormalTerminations
              .labels(
                label(classifier),
                AbnormalTermination.report(AbnormalTermination.Timeout),
                label(Option.empty)
              )
              .observe(SimpleTimer.elapsedSecondsFromNanos(0, elapsed))
          }

        override def recordTotalBytesBilled(
            job: Option[JobStatistics],
            classifier: Option[String]
        ): F[Unit] =
          job
            .collect { case stats: QueryStatistics =>
              stats.getTotalBytesBilled
            }
            .map(totalBytesBilled =>
              F.delay {
                metrics.bytesBilled
                  .labels(
                    label(classifier)
                  )
                  .inc(totalBytesBilled.toDouble)
              }
            )
            .getOrElse(F.unit)

        private def label(value: Option[String]): String = value.getOrElse("")

      }

    private def createMetricsCollection[F[_]: Sync](
        registry: CollectorRegistry,
        prefix: String,
        responseDurationSecondsHistogramBuckets: NonEmptyList[Double]
    ): Resource[F, MetricsCollection] = {
      val responseDuration: Resource[F, Histogram] = registerCollector(
        Histogram
          .build()
          .buckets(responseDurationSecondsHistogramBuckets.toList: _*)
          .name(prefix + "_" + "response_duration_seconds")
          .help("Response Duration in seconds.")
          .labelNames("classifier")
          .create(),
        registry
      )

      val activeRequests: Resource[F, Gauge] = registerCollector(
        Gauge
          .build()
          .name(prefix + "_" + "active_request_count")
          .help("Total Active Requests.")
          .labelNames("classifier")
          .create(),
        registry
      )

      val requests: Resource[F, Counter] = registerCollector(
        Counter
          .build()
          .name(prefix + "_" + "request_count")
          .help("Total Requests.")
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

      val bytesBilled: Resource[F, Gauge] = registerCollector(
        Gauge
          .build()
          .name(prefix + "_" + "bytes_billed")
          .help("Total bytes billed.")
          .labelNames("classifier")
          .create(),
        registry
      )

      (
        responseDuration,
        activeRequests,
        requests,
        abnormalTerminations,
        bytesBilled
      ).mapN(MetricsCollection.apply)
    }
  }

  private def registerCollector[F[_], C <: Collector](
      collector: C,
      registry: CollectorRegistry
  )(implicit F: Sync[F]): Resource[F, C] =
    Resource.make(F.blocking(collector.register[C](registry)))(c =>
      F.blocking(registry.unregister(c))
    )

  // https://github.com/prometheus/client_java/blob/parent-0.6.0/simpleclient/src/main/java/io/prometheus/client/Histogram.java#L73
  private val DefaultHistogramBuckets: NonEmptyList[Double] =
    NonEmptyList(
      .005,
      List(.01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10)
    )
}

final case class MetricsCollection(
    responseDuration: Histogram,
    activeRequests: Gauge,
    requests: Counter,
    abnormalTerminations: Histogram,
    bytesBilled: Gauge
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
      case Timeout  => "timeout"
      case Error    => "error"
      case Canceled => "cancel"
    }
}
