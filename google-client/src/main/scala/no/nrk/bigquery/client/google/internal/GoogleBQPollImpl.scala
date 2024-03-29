/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.client.google.internal

import com.google.cloud.bigquery.*
import no.nrk.bigquery.*

import scala.jdk.CollectionConverters.*

private[client] object GoogleBQPollImpl {
  implicit val instance: BQPoll.FromJob[Job] = new BQPoll.FromJob[Job] {
    override def reference(job: Job): BQJobId =
      GoogleTypeHelper.jobIdFromJob(job)

    override def toPoll(job: Job): BQPoll = {
      val maybeStatus: Option[JobStatus] =
        Option(job.getStatus)

      val maybeFailed: Option[BQPoll.Failed] =
        maybeStatus.flatMap { (s: JobStatus) =>
          def toErr(err: BigQueryError) =
            BQError(Option(err.getLocation), Option(err.getMessage), Option(err.getReason))

          val primary: Option[BQError] =
            Option(s.getError).map(toErr)

          val details: List[BQError] =
            (Option(s.getExecutionErrors) match {
              case Some(values) => values.asScala.toList
              case None => Nil
            }).map(toErr)

          if (primary.isEmpty && details.isEmpty) None
          else Some(BQPoll.Failed(BQExecutionException(reference(job), primary, details)))
        }

      maybeFailed.getOrElse {
        maybeStatus match {
          case Some(status) =>
            status.getState match {
              case JobStatus.State.DONE => BQPoll.Success(job)
              case JobStatus.State.PENDING => BQPoll.Pending
              case JobStatus.State.RUNNING => BQPoll.Running
              case _ => BQPoll.Unknown
            }
          case None => BQPoll.Unknown
        }
      }
    }
  }
}
