/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import no.nrk.bigquery.syntax._

import scala.annotation.tailrec

object mergeQuery {
  def into[Pid <: BQPartitionId[Any]](source: Pid, target: Pid, primaryKey: Ident, morePrimaryKeys: Ident*): BQSqlFrag =
    (source.wholeTable, target.wholeTable) match {
      case (from: BQTableDef[Any], to: BQTableDef[Any]) =>
        into(from, to, primaryKey, morePrimaryKeys *)
      case (from, into) =>
        sys.error(s"Cannot merge $from into $into")
    }

  def into[P](
      source: BQTableDef[P],
      target: BQTableDef[P],
      primaryKey: Ident,
      morePrimaryKeys: Ident*
  ): BQSqlFrag =
    // compare schema without comments, since those may be dynamic and it doesnt matter anyway
    if (BQType.fromBQSchema(source.schema) != BQType.fromBQSchema(target.schema)) {
      sys.error(s"Cannot merge $source into $target")
    } else {
      val partitionField = target.partitionType match {
        case BQPartitionType.DatePartitioned(field) => Some(field)
        case BQPartitionType.RangePartitioned(field, _) => Some(field)
        case _ => None
      }

      val primaryKeys: Seq[Ident] =
        List(
          List(primaryKey),
          morePrimaryKeys.toList,
          partitionField.toList
        ).flatten.distinct

      val allFields: List[BQField] =
        source.schema.fields

      val allFieldNames: List[Ident] =
        allFields.map(_.ident)

      val isPrimaryKey = primaryKeys.toSet
      val (declareStruct, prunePartitions) = partitionPruningFrags(source)

      bqsql"""
             |$declareStruct
             |
             |MERGE ${target.wholeTable} AS T
             |USING ${source.wholeTable} AS S
             |ON ${primaryKeys.toList
          .map(keyEqualsFragment(allFields))
          .mkFragment("\n AND ")}
             |$prunePartitions
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

  def partitionPruningFrags[P](source: BQTableDef[P]): (BQSqlFrag, BQSqlFrag) =
    (source.partitionType match {
      case BQPartitionType.DatePartitioned(field) => Some((field, BQType.DATE))
      case BQPartitionType.MonthPartitioned(field) => Some((field, BQType.DATE))
      case BQPartitionType.RangePartitioned(field, _) => Some((field, BQType.INT64))
      case _ => None
    }).fold((BQSqlFrag.Empty, BQSqlFrag.Empty)) { case (field, fieldType) =>
      (
        bqsql"""
             |DECLARE partitions STRUCT<minP $fieldType, maxP $fieldType>;
             |SET partitions = (SELECT STRUCT(MIN($field) AS minP , MAX($field) AS maxP) FROM ${source.wholeTable});
        """.stripMargin,
        bqsql"AND T.$field BETWEEN partitions.minP AND partitions.maxP"
      )
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
