package no.nrk.bigquery

import cats.Show
import cats.syntax.show._
import com.google.cloud.bigquery.{BigQueryError, JobId, Jsonify}
import BQExecutionExceptionInstances._

case class BQExecutionException(
    jobId: JobId,
    main: Option[BigQueryError],
    details: List[BigQueryError]
) extends Exception(
      show"Error while executing job $jobId: $main, details: ${details.show}"
    )


private object BQExecutionExceptionInstances {
  implicit val showJobId: Show[JobId] = Show.show(Jsonify.jobId)
  implicit val showBigQueryError: Show[BigQueryError] = Show.show(Jsonify.error)
}