/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package no.nrk.bigquery

case class BQSchema(fields: List[BQField]) {

  /** This is useful for views, because we're unable to create views with required columns.
    *
    * This is enforced by BQ at create-time because we *have* to create the view without a schema, and then we may
    * optionally update the view to add our schema, and in that case it can only legally extend what was inferred.
    *
    * @return
    *   a version of the BQSchema with all fields set to nullable. repeated fields are still allowed.
    */
  def recursivelyNullable: BQSchema =
    copy(fields = fields.map(_.recursivelyNullable))

  /** Return all fields that are required. If a struct has required fields, but is not itself required, returns that.
    * This is a companion to [[recursivelyNullable]]
    */
  def requiredFields: List[BQField] = {
    def go(field: BQField, list: List[BQField]): List[BQField] = {
      val children = field.subFields.flatMap(go(_, list))
      if (children.nonEmpty || field.isRequired) {
        field :: list
      } else {
        list
      }
    }

    fields.flatMap(go(_, Nil))
  }

  def extend(additional: BQSchema): BQSchema = BQSchema(fields ::: additional.fields)

  def extendWith(additional: BQField*): BQSchema = BQSchema(fields ::: additional.toList)

  def filter(predicate: BQField => Boolean): BQSchema = BQSchema(fields.filter(predicate))

  def filterNot(predicate: BQField => Boolean): BQSchema = BQSchema(fields.filterNot(predicate))

}

object BQSchema {
  def of(fields: BQField*) = new BQSchema(fields.toList)
}
