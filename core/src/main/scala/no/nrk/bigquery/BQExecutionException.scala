package no.nrk.bigquery

import cats.syntax.show._
import com.google.cloud.bigquery.{BigQueryError, JobId}

case class BQExecutionException(jobId: JobId, main: Option[BigQueryError], details: List[BigQueryError])
    extends Exception(show"Error while executing job $jobId: $main, details: ${details.show}")
