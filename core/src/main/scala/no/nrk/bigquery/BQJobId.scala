/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

final case class BQJobId(projectId: Option[ProjectId], locationId: Option[LocationId], name: BQJobName) {
  def withProjectId(projectId: ProjectId) = copy(projectId = Some(projectId))
  def withLocationId(locationId: LocationId) = copy(locationId = Some(locationId))
  private[bigquery] def withDefaults(defaults: Option[BQClientDefaults]) =
    if (projectId.isEmpty && locationId.isEmpty) {
      copy(projectId = defaults.map(_.projectId), locationId = defaults.map(_.locationId))
    } else this

  def +(str: String): BQJobId = copy(name = name + str)
}

object BQJobId {
  def auto(implicit enclosing: sourcecode.Enclosing) =
    BQJobId(None, None, BQJobName(enclosing.value))

  def apply(name: String): BQJobId = BQJobId(None, None, BQJobName(name))
}
