/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package com.google.cloud.bigquery

import scala.Option

object RoutineInfoHelper {
  def setDescription(info: RoutineInfo.Builder)(value: Option[String]) = {
    value.foreach(v => info.setDescription(v))
    info
  }

}
