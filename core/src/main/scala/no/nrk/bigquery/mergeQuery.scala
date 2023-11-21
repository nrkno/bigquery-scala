/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.data.NonEmptyList
import no.nrk.bigquery.syntax.*

import scala.annotation.tailrec

object mergeQuery {
  def into[Pid <: BQPartitionId[Any]](
      source: Pid,
      target: Pid,
      primaryKey: Ident,
      morePrimaryKeys: Ident*
  ): BQSqlFrag =
    intoTargets(source, NonEmptyList.one(target), primaryKey, morePrimaryKeys)

  def intoTargets[Pid <: BQPartitionId[Any]](
      source: Pid,
      targets: NonEmptyList[Pid],
      primaryKey: Ident,
      morePrimaryKeys: Seq[Ident]
  ): BQSqlFrag = {
    val targetTable = targets.head.wholeTable
    val primaryKeys: Seq[Ident] = {
      val partitionField: Option[Ident] =
        targetTable.partitionType match {
          case BQPartitionType.DatePartitioned(field) => Some(field)
          case _ => None
        }

      List(
        List(primaryKey),
        morePrimaryKeys.toList,
        partitionField.toList
      ).flatten.distinct
    }

    val allFields: List[BQField] =
      (source.wholeTable, targetTable) match {
        // compare schema without comments, since those may be dynamic and it doesnt matter anyway
        case (from: BQTableDef[Any], into: BQTableDef[Any])
            if BQType
              .fromBQSchema(from.schema) == BQType.fromBQSchema(into.schema) =>
          from.schema.fields
        case (from, into) =>
          sys.error(s"Cannot merge $from into $into")
      }

    val allFieldNames: List[Ident] =
      allFields.map(_.ident)

    val isPrimaryKey = primaryKeys.toSet

    // note: we need to specify whole table for target table. partition info will be inferred from source
    bqsql"""
           |MERGE ${targetTable.unpartitioned} AS T
           |USING $source AS S
           |ON ${primaryKeys.toList
        .map(keyEqualsFragment(allFields))
        .mkFragment("\n AND ")}
           |AND ${targets.map(p => p.partitionQuery(Option("T."))).mkFragment("(", "OR", ")")}
           |WHEN MATCHED THEN UPDATE SET
           |${allFieldNames
        .filterNot(isPrimaryKey)
        .map(nonKey => bqfr"    T.$nonKey = S.$nonKey")
        .mkFragment(",\n")}
           |WHEN NOT MATCHED THEN
           |  INSERT (
           |${allFieldNames.map(field => bqfr"    $field").mkFragment(",\n")}
           |  )
           |  VALUES (
           |${allFieldNames.map(field => bqfr"    S.$field").mkFragment(",\n")}
           |  )
         """.stripMargin
  }

  def keyEqualsFragment(
      allFields: List[BQField]
  )(primaryKey: Ident): BQSqlFrag =
    if (isOptional(allFields, primaryKey))
      bqfr"(T.$primaryKey = S.$primaryKey OR (T.$primaryKey IS NULL AND S.$primaryKey IS NULL))"
    else
      bqfr"T.$primaryKey = S.$primaryKey"

  def isOptional(allFields: List[BQField], ident: Ident): Boolean = {
    @tailrec
    def go(fields: List[BQField], ident: List[String]): Boolean =
      ident match {
        case Nil => false
        case current :: tail =>
          fields.find(_.name == current) match {
            case Some(field) =>
              if (field.mode == BQField.Mode.NULLABLE) true
              else go(field.subFields, tail)
            case None =>
              sys.error(
                s"Couldn't resolve ${ident.mkString(".")} among ${fields.map(_.name)}"
              )
          }
      }

    go(allFields, ident.value.split("\\.").toList)
  }
}
