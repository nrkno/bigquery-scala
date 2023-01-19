package no.nrk.bigquery

import cats.Applicative
import com.google.cloud.bigquery.JobStatistics

import scala.concurrent.duration.FiniteDuration

trait BQTracker[F[_]] {
  def apply(
      duration: FiniteDuration,
      jobName: BQJobName,
      isSuccess: Boolean,
      stats: Option[JobStatistics]
  ): F[Unit]
}

object BQTracker {
  class Noop[F[_]](implicit F: Applicative[F]) extends BQTracker[F] {
    override def apply(
        duration: FiniteDuration,
        jobName: BQJobName,
        isSuccess: Boolean,
        stats: Option[JobStatistics]
    ): F[Unit] = F.unit
  }
}
