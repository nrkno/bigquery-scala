package no.nrk.bigquery

import cats.effect.IO
import com.google.cloud.bigquery.JobStatistics

import scala.concurrent.duration.FiniteDuration

trait BQTracker {
  def apply(
      duration: FiniteDuration,
      jobName: BQJobName,
      isSuccess: Boolean,
      stats: Option[JobStatistics]
  ): IO[Unit]
}

object BQTracker {
  object Noop extends BQTracker {
    override def apply(
        duration: FiniteDuration,
        jobName: BQJobName,
        isSuccess: Boolean,
        stats: Option[JobStatistics]
    ): IO[Unit] = IO.unit
  }
}
