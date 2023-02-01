package no.nrk.bigquery.metrics

import com.google.cloud.bigquery.JobStatistics

trait MetricsOps[F[_]] {
  def increaseActiveRequests(classifier: Option[String]): F[Unit]
  def decreaseActiveRequests(classifier: Option[String]): F[Unit]
  def recordTotalTime(elapsed: Long, classifier: Option[String]): F[Unit]
  def recordAbnormalTermination(
      elapsed: Long,
      terminationType: TerminationType,
      classifier: Option[String]
  ): F[Unit]
  def recordTotalBytesBilled(
      job: Option[JobStatistics],
      classifier: Option[String]
  ): F[Unit]
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
