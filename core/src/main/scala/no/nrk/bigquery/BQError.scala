/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import io.circe.{Encoder, Json}
import io.circe.syntax.*

final case class BQError(
    // Specifies where the error occurred, if present.
    location: Option[String] = None,
    // A human-readable description of the error.
    message: Option[String] = None,
    // A short error code that summarizes the error.
    reason: Option[String] = None
)

object BQError {
  implicit val encoder: Encoder[BQError] = Encoder.instance { x =>
    Json.obj(
      "location" := x.location,
      "message" := x.message,
      "reason" := x.reason
    )
  }
}

case class BQExecutionException(
    jobId: BQJobId,
    main: Option[BQError],
    details: List[BQError]
) extends Exception(
      s"Error while executing job $jobId: ${main.asJson.spaces4}, details: ${details.asJson.spaces4}"
    )
