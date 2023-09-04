/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.google.cloud.bigquery

import com.google.api.client.json.gson.GsonFactory

// need to place this inside `com.google.cloud.bigquery` to get access to `toPb`
object Jsonify {
  private val factory = GsonFactory.getDefaultInstance

  def job(job: JobInfo): String = {
    val pb = job.toPb

    // drop these, it's too much logging and too little useful
    if (pb.getStatistics != null && pb.getStatistics.getQuery != null) {
      pb.getStatistics.getQuery.setQueryPlan(null)
      pb.getStatistics.getQuery.setTimeline(null)
      ()
    }

    // also shorten very long queries
    if (pb.getConfiguration != null && pb.getConfiguration.getQuery != null) {
      pb.getConfiguration.getQuery.setQuery(
        shorten(1000)(pb.getConfiguration.getQuery.getQuery)
      )
      ()
    }

    factory.toString(pb)
  }

  def jobId(jobId: JobId): String =
    factory.toString(jobId.toPb)

  def error(e: BigQueryError): String =
    factory.toString(e.toPb)

  private def shorten(max: Int)(str: String): String =
    if (str.length < max) str
    else str.take(max - 3) + "..."
}
