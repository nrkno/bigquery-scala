package no.nrk.bigquery.internal

import cats.Show
import com.google.cloud.bigquery.{BigQueryError, JobId, JobInfo, Jsonify}

trait CatsShowInstances {

  // orphan instances below
  implicit val showJobId: Show[JobId] = Show.show(Jsonify.jobId)
  implicit val showBigQueryError: Show[BigQueryError] = Show.show(Jsonify.error)

  implicit def showJob[J <: JobInfo]: Show[J] = Show.show(Jsonify.job)
}
