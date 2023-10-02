/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.effect.Async
import cats.effect.implicits._
import cats.syntax.all._
import com.google.cloud.bigquery.{BigQueryError, Job, JobStatus}
import org.typelevel.log4cats.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.Random

sealed trait BQPoll

object BQPoll {
  sealed trait NotFinished extends BQPoll
  case object Unknown extends NotFinished
  case object Pending extends NotFinished
  case object Running extends NotFinished

  sealed trait Finished extends BQPoll
  case class Failed(error: BQExecutionException) extends Finished
  case class Success(job: Job) extends Finished

  def poll[F[_]](
      runningJob: Job,
      baseDelay: FiniteDuration,
      maxDuration: FiniteDuration,
      maxErrorsTolerated: Int
  )(
      retry: F[Job]
  )(implicit F: Async[F], lf: LoggerFactory[F]): F[BQPoll.Finished] = {
    val logger = lf.getLogger
    def go(
        runningJob: Job,
        seenErrors: List[Throwable],
        seenNotFinished: List[BQPoll.NotFinished]
    ): F[BQPoll.Finished] =
      fromJob(runningJob) match {
        case x: BQPoll.Finished => F.pure(x)
        case notFinished: BQPoll.NotFinished =>
          val jobId = runningJob.getJobId
          val newSeenNotFinished = notFinished :: seenNotFinished
          val waitFor = fullJitter(baseDelay, seenErrors.length)

          logger.info(
            s"sleeping ${waitFor.toMillis}ms before polling ${jobId.getJob}. Current status $notFinished"
          ) >> F.sleep(waitFor) >> retry.attempt
            .flatMap {
              case Left(error) =>
                val newSeenErrors = error :: seenErrors

                if (newSeenErrors.length == maxErrorsTolerated) {
                  F.raiseError(error) // will be logged later
                } else {
                  logger.info(error)(
                    s"Network error while polling $jobId. Retrying"
                  ) >>
                    go(runningJob, newSeenErrors, newSeenNotFinished)
                }

              case Right(runningJob) =>
                go(runningJob, seenErrors, newSeenNotFinished)
            }
      }

    F.sleep(maxDuration).race(go(runningJob, Nil, Nil)).flatMap {
      case Left(_) =>
        F.raiseError(BQExecutionException(runningJob.getJobId, None, Nil))
      case Right(finished) => F.pure(finished)
    }
  }

  def fullJitter(
      baseDelay: FiniteDuration,
      retriesSoFar: Int
  ): FiniteDuration = {
    val e = Math.pow(2, retriesSoFar.toDouble).toLong
    val maxDelay = safeMultiply(baseDelay, e)
    val delayNanos = (maxDelay.toNanos * Random.nextDouble()).toLong
    new FiniteDuration(delayNanos, TimeUnit.NANOSECONDS)
  }

  private val LongMax: BigInt = BigInt(Long.MaxValue)

  /*
   * Multiply the given duration by the given multiplier, but cap the result to
   * ensure we don't try to create a FiniteDuration longer than 2^63 - 1 nanoseconds.
   *
   * Note: despite the "safe" in the name, we can still create an invalid
   * FiniteDuration if the multiplier is negative. But an assumption of the library
   * as a whole is that nobody would be silly enough to use negative delays.
   */
  private def safeMultiply(
      duration: FiniteDuration,
      multiplier: Long
  ): FiniteDuration = {
    val durationNanos = BigInt(duration.toNanos)
    val resultNanos = durationNanos * BigInt(multiplier)
    val safeResultNanos = resultNanos.min(LongMax)
    FiniteDuration(safeResultNanos.toLong, TimeUnit.NANOSECONDS)
  }

  def fromJob(pulledJob: Job): BQPoll = {
    val jobId = pulledJob.getJobId

    val maybeStatus: Option[JobStatus] =
      Option(pulledJob.getStatus)

    val maybeFailed: Option[BQPoll.Failed] =
      maybeStatus.flatMap { (s: JobStatus) =>
        val primary: Option[BigQueryError] =
          Option(s.getError)

        val details: List[BigQueryError] =
          Option(s.getExecutionErrors) match {
            case Some(values) => values.asScala.toList
            case None => Nil
          }

        if (primary.isEmpty && details.isEmpty) None
        else Some(BQPoll.Failed(BQExecutionException(jobId, primary, details)))
      }

    maybeFailed.getOrElse {
      maybeStatus match {
        case Some(status) =>
          status.getState match {
            case JobStatus.State.DONE => BQPoll.Success(pulledJob)
            case JobStatus.State.PENDING => BQPoll.Pending
            case JobStatus.State.RUNNING => BQPoll.Running
            case _ => BQPoll.Unknown
          }
        case None => BQPoll.Unknown
      }
    }
  }
}
