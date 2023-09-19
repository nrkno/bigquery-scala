/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package internal

import com.google.cloud.bigquery.{Option => _, _}

import scala.jdk.CollectionConverters._

object SchemaHelper {
  def fromType(typ: BQField.Type): StandardSQLTypeName = StandardSQLTypeName.valueOf(typ.name)
  def fromMode(mode: BQField.Mode): Field.Mode = Field.Mode.valueOf(mode.name)

  def toSchema(schema: BQSchema): Schema =
    Schema.of(schema.fields.map(toField): _*)

  sealed trait TableConversionError
  object TableConversionError {
    final case class IllegalTableId(msg: String) extends TableConversionError
    final case class UnsupportedPartitionType(msg: String) extends TableConversionError
    final case class UnsupportedTableType(msg: String) extends TableConversionError
  }

  def fromTable(table: TableInfo): Either[TableConversionError, BQTableDef[Any]] = {
    val id = List(
      Option(table.getTableId.getProject),
      Option(table.getTableId.getDataset),
      Option(table.getTableId.getTable)).flatten.mkString(".")
    val parsedId = BQTableId.fromString(id).left.map(msg => TableConversionError.IllegalTableId(msg))

    table.getDefinition[TableDefinition] match {
      case st: StandardTableDefinition =>
        for {
          typ <- PartitionTypeHelper.from(st).left.map(msg => TableConversionError.UnsupportedPartitionType(msg))
          tableId <- parsedId
        } yield BQTableDef.Table(
          tableId = tableId,
          schema = fromSchema(st.getSchema),
          partitionType = typ,
          description = Option(table.getDescription),
          clustering = Option(st.getClustering).toList
            .flatMap(_.getFields.asScala)
            .map(Ident.apply),
          labels = GoogleTypeHelper.tableLabelsfromTableInfo(table),
          tableOptions = GoogleTypeHelper.toTableOptions(table)
        )

      case v: ViewDefinition =>
        parsedId.map(tableId =>
          BQTableDef.View(
            tableId = tableId,
            schema = fromSchema(v.getSchema),
            partitionType = BQPartitionType.NotPartitioned,
            description = Option(table.getDescription),
            labels = GoogleTypeHelper.tableLabelsfromTableInfo(table),
            query = BQSqlFrag.Frag(v.getQuery)
          ))

      case v: MaterializedViewDefinition =>
        for {
          typ <- PartitionTypeHelper.from(v).left.map(msg => TableConversionError.UnsupportedPartitionType(msg))
          tableId <- parsedId

        } yield BQTableDef.MaterializedView(
          tableId = tableId,
          schema = fromSchema(v.getSchema),
          partitionType = typ,
          description = Option(table.getDescription),
          enableRefresh = Option(v.getEnableRefresh).forall(_.booleanValue()),
          refreshIntervalMs = Option(v.getRefreshIntervalMs).map(_.longValue()).getOrElse(1800000L),
          /*
          TODO: Add clustering to BQTableDef.MaterializedView

          clustering = Option(st.getClustering).toList
                .flatMap(_.getFields.asScala)
                .map(Ident.apply),*/
          labels = GoogleTypeHelper.tableLabelsfromTableInfo(table),
          query = BQSqlFrag.Frag(v.getQuery),
          tableOptions = GoogleTypeHelper.toTableOptions(table)
        )

      case s => Left(TableConversionError.UnsupportedTableType(s"${s.getType.name()} is not supported here"))
    }
  }

  def fromSchema(schema: Schema): BQSchema =
    schema match {
      case null => BQSchema(Nil)
      case schema =>
        BQSchema(schema.getFields.asScala.toList.map(fromField))
    }

  def toField(field: BQField): Field = {
    val b = Field.newBuilder(field.name, fromType(field.tpe), field.subFields.map(toField): _*)
    b.setMode(fromMode(field.mode))
    field.description.foreach(b.setDescription)
    if (field.policyTags.nonEmpty) {
      b.setPolicyTags(
        PolicyTags.newBuilder().setNames(field.policyTags.asJava).build()
      )
      ()
    }
    b.build()
  }

  def fromField(f: Field): BQField =
    BQField(
      name = f.getName,
      mode = mapMode(f.getMode),
      tpe = BQField.Type.unsafeFromString(f.getType.getStandardType.name()),
      description = mapDescription(f.getDescription),
      subFields = mapSubFields(f.getSubFields),
      policyTags = Option(f.getPolicyTags)
        .flatMap(pt => Option(pt.getNames))
        .fold(List.empty[String])(_.asScala.toList)
    )

  def mapMode(mode: Field.Mode): BQField.Mode =
    BQField.Mode.unsafeFromString((mode match {
      case null => Field.Mode.NULLABLE
      case other => other
    }).name())

  def mapDescription(description: String): Option[String] =
    description match {
      case null => None
      case "" => None
      case other => Some(other)
    }

  def mapSubFields(fs: FieldList): List[BQField] =
    Option(fs) match {
      case Some(fs) => fs.asScala.toList.map(fromField)
      case None => List.empty
    }

  def bqTypeFromSchema(schema: Schema): BQType =
    BQType.fromBQSchema(fromSchema(schema))
}
