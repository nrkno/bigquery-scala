/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery.client.http4s.internal

import googleapis.bigquery.{Job, JobStatus}
import no.nrk.bigquery.*

private[client] object Http4sBQPollImpl {
  implicit val instance: BQPoll.FromJob[Job] = new BQPoll.FromJob[Job] {
    override def reference(job: Job): BQJobId =
      JobHelper.jobId(job).getOrElse(throw new IllegalStateException("Unable to get id from job"))

    override def id(job: Job): Option[String] =
      job.id

    override def toPoll(job: Job): BQPoll = {
      val maybeStatus: Option[JobStatus] = job.status

      val maybeFailed: Option[BQPoll.Failed] =
        maybeStatus.flatMap { (s: JobStatus) =>
          val primary = s.errorResult.map(proto => BQError(proto.location, proto.message, proto.reason))

          val details = s.errors.getOrElse(Nil).map(proto => BQError(proto.location, proto.message, proto.reason))

          if (primary.isEmpty && details.isEmpty) None
          else Some(BQPoll.Failed(BQExecutionException(reference(job), primary, details)))
        }

      maybeFailed.getOrElse {
        maybeStatus match {
          case Some(status) =>
            status.state match {
              case Some("DONE") => BQPoll.Success(job)
              case Some("PENDING") => BQPoll.Pending
              case Some("RUNNING") => BQPoll.Running
              case _ => BQPoll.Unknown
            }
          case None => BQPoll.Unknown
        }
      }
    }
  }
}
