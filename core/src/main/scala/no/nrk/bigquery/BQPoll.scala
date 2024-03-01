/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.syntax.all.*
import cats.effect.Async
import cats.effect.implicits.*
import org.typelevel.log4cats.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

sealed trait BQPoll

object BQPoll {
  sealed trait NotFinished extends BQPoll

  case object Unknown extends NotFinished
  case object Pending extends NotFinished
  case object Running extends NotFinished

  sealed trait Finished[+Job] extends BQPoll
  case class Failed(error: BQExecutionException) extends Finished[Nothing]
  case class Success[Job](job: Job) extends Finished[Job]

  trait FromJob[Job] {
    def reference(job: Job): BQJobId
    def id(job: Job): Option[String]
    def toPoll(job: Job): BQPoll
  }

  private[bigquery] class Poller[F[_]](config: QueryClient.PollConfig)(implicit F: Async[F], lf: LoggerFactory[F]) {
    def poll[Job](runningJob: Job, retry: F[Option[Job]])(implicit fromJob: FromJob[Job]): F[BQPoll.Finished[Job]] = {
      val logger = lf.getLogger
      def go(
          runningJob: Job,
          seenErrors: List[Throwable],
          seenNotFinished: List[BQPoll.NotFinished]
      ): F[BQPoll.Finished[Job]] =
        fromJob.toPoll(runningJob) match {
          case x: BQPoll.Finished[?] => F.pure(x.asInstanceOf[BQPoll.Finished[Job]])
          case notFinished: BQPoll.NotFinished =>
            val jobId = fromJob.id(runningJob)
            val newSeenNotFinished = notFinished :: seenNotFinished
            val waitFor = fullJitter(config.baseDelay, seenErrors.length)

            logger.info(
              s"sleeping ${waitFor.toMillis}ms before polling ${jobId.getOrElse("")}. Current status $notFinished"
            ) >> F.sleep(waitFor) >> retry.attempt
              .flatMap {
                case Left(error) =>
                  val newSeenErrors = error :: seenErrors

                  if (newSeenErrors.length == config.maxErrorsTolerated) {
                    F.raiseError(error) // will be logged later
                  } else {
                    logger.info(error)(
                      s"Network error while polling $jobId. Retrying"
                    ) >>
                      go(runningJob, newSeenErrors, newSeenNotFinished)
                  }

                case Right(Some(runningJob)) =>
                  go(runningJob, seenErrors, newSeenNotFinished)
                case Right(None) =>
                  go(runningJob, seenErrors, seenNotFinished)
              }
        }

      F.sleep(config.maxDuration).race(go(runningJob, Nil, Nil)).flatMap {
        case Left(_) =>
          F.raiseError(BQExecutionException(fromJob.reference(runningJob), None, Nil))
        case Right(finished) => F.pure(finished)
      }
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
}
