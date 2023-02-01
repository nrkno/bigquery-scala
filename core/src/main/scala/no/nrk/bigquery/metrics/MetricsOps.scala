package no.nrk.bigquery.metrics

import cats.effect.Sync
import cats.effect.kernel.Resource
import com.google.cloud.bigquery.JobStatistics
import no.nrk.bigquery.BQJobName

trait MetricsOps[F[_]] {
  def increaseActiveRequests(jobName: BQJobName): F[Unit]
  def decreaseActiveRequests(jobName: BQJobName): F[Unit]
  def recordTotalTime(elapsed: Long, jobName: BQJobName): F[Unit]
  def recordAbnormalTermination(
      elapsed: Long,
      terminationType: TerminationType,
      jobName: BQJobName
  ): F[Unit]
  def recordTotalBytesBilled(
      job: Option[JobStatistics],
      jobName: BQJobName
  ): F[Unit]
}

object MetricsOps {
  def NoopMetricsOps[F[_]](implicit F: Sync[F]): Resource[F, MetricsOps[F]] =
    Resource.pure(new MetricsOps[F] {
      override def increaseActiveRequests(jobName: BQJobName): F[Unit] =
        F.unit

      override def decreaseActiveRequests(jobName: BQJobName): F[Unit] =
        F.unit

      override def recordTotalTime(
          elapsed: Long,
          jobName: BQJobName
      ): F[Unit] = F.unit

      override def recordAbnormalTermination(
          elapsed: Long,
          terminationType: TerminationType,
          jobName: BQJobName
      ): F[Unit] = F.unit

      override def recordTotalBytesBilled(
          job: Option[JobStatistics],
          jobName: BQJobName
      ): F[Unit] = F.unit
    })
}

/** Describes the type of abnormal termination */

sealed trait TerminationType
object TerminationType {

  /** Signals just a generic abnormal termination */
  case class Abnormal(rootCause: Throwable) extends TerminationType

  /** Signals cancelation */
  case object Canceled extends TerminationType

  /** Signals an abnormal termination due to an error processing the request,
    * either at the server or client side
    */
  case class Error(rootCause: Throwable) extends TerminationType

  /** Signals a client timing out during a request */
  case object Timeout extends TerminationType

}
