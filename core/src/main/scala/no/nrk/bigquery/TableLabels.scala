/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.data.NonEmptyChain
import cats.syntax.all.*

import scala.collection.immutable.SortedMap

/** @param values sorted for consistent behaviour */
final case class TableLabels private[bigquery] (values: SortedMap[String, String]) {
  def withAll(moreLabels: Iterable[(Labels.Key, Labels.Value)]): TableLabels =
    TableLabels(values ++ moreLabels.map { case (k, v) => k.value -> v.value })

  def ++(other: TableLabels): TableLabels =
    TableLabels(values ++ other.values)

  def contains(tableLabels: TableLabels): Boolean =
    if (tableLabels.values.nonEmpty) {
      tableLabels.values.forall { case (key, value) =>
        values.get(key).contains(value)
      }
    } else false

  /** This method is needed for the case where we delete a label. It is deleted by setting it to `null`.
    *
    * As such, we need to know the labels of a table in production before we compute the new set of labels to use when
    * updating
    */
  def forUpdate(
      maybeExistingTable: Option[BQTableDef[Any]]
  ): java.util.Map[String, String] = {
    val ret = new java.util.TreeMap[String, String]

    // existing table? set all old label keys to `null`
    for {
      existingTable <- maybeExistingTable.toList
      existingKey <- existingTable.labels.values.keySet
    } ret.put(existingKey, null)

    // and overwrite the ones we want to keep.
    values.foreach { case (key, value) => ret.put(key, value) }

    ret
  }
}

object TableLabels {
  val Empty: TableLabels = new TableLabels(SortedMap.empty)

  def validated(values: Iterable[(String, String)]): Either[NonEmptyChain[String], TableLabels] =
    values.toList
      .traverse { case (key, value) =>
        (Labels.Key.apply(key), Labels.Value.apply(value)).tupled
      }
      .map(list => TableLabels.from(list))
      .toEither

  def from(params: scala.collection.immutable.Seq[(Labels.Key, Labels.Value)]): TableLabels = TableLabels(
    SortedMap(params.map { case (k, v) =>
      k.value -> v.value
    }: _*))

  /* nicer syntax for creating instances */
  def apply(values: (String, String)*): TableLabels =
    validated(values).fold(err => throw new IllegalArgumentException(err.toList.mkString("\n")), identity)

}
