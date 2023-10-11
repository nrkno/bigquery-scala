/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery
package example

import org.apache.commons.text.translate.LookupTranslator

import java.nio.charset.StandardCharsets
import java.nio.file.*
import scala.collection.immutable
import scala.jdk.CollectionConverters._

object Generators {
  private val escaper = new LookupTranslator(
    Map[CharSequence, CharSequence]("\"" -> "\\\"", "\\" -> "\\\\", "$" -> "$$").asJava)

  private def lit(v: String) = s""""$v""""

  private def description(desc: Option[String]) =
    desc.map(s => s"Some(${lit(s)})").getOrElse("None")

  def genSchema(schema: BQSchema) = {
    val outerIndent = repeat(4)(" ")

    def genField(indent: Int, field: BQField): String = {
      val level = repeat(indent)(" ")
      val desc = description(field.description)
      val gen = if (field.tpe == BQField.Type.STRUCT) {
        val subfields = field.subFields.map(f => genField(indent + 2, f)).mkString(",\n")
        s"BQField.struct(${lit(field.name)}, BQField.Mode.${field.mode.name}, ${desc})(\n$subfields\n$level)"
      } else {
        s"BQField(${lit(field.name)}, BQField.Type.${field.tpe.name}, BQField.Mode.${field.mode.name}, ${desc})"
      }
      level + gen
    }
    s"""BQSchema.of(
       |${schema.fields.map(genField(6, _)).mkString("", ",\n", "")}
       |$outerIndent)""".stripMargin
  }

  def repeat(n: Int)(rep: String) = List.fill(n)(rep).mkString

  def genQuery(sql: String) = {
    val indent = repeat(14 + 8)(" ")

    def escape(line: String) =
      escaper.translate(line)

    val query2 = {
      val split = sql.split("\n")
      val head = "|" + escape(split.head) + "\n"
      head + split.tail.map(line => indent + "|" + escape(line)).mkString("\n")
    }

    val quoted = "\"\"\""
    s"bqsql$quoted$query2$quoted.stripMargin"
  }

  def genPartitionType(partitionType: BQPartitionType[Any]): String =
    partitionType match {
      case BQPartitionType.DatePartitioned(Ident(field)) =>
        s"BQPartitionType.DatePartitioned(Ident(${lit(field)}))"
      case BQPartitionType.MonthPartitioned(Ident(field)) =>
        s"BQPartitionType.MonthPartitioned(Ident(${lit(field)}))"
      case BQPartitionType.Sharded =>
        "BQPartitionType.Sharded"
      case _: BQPartitionType.NotPartitioned =>
        "BQPartitionType.NotPartitioned"
    }

  // todo: can we use pprint for this?
  def genViewDef(view: BQTableDef.View[Any]) = {
    val partitionType = genPartitionType(view.partitionType)
    val labels =
      if (view.labels.values.isEmpty) "TableLabels.Empty"
      else view.labels.values.map { case (k, v) => s"${lit(k)} -> ${lit(v)}" }.mkString("TableLabels(", ", ", ")")
    val outerLevel = repeat(2)(" ")
    val level = repeat(4)(" ")
    s"""§BQTableDef.View(
        §${level}tableId = BQTableId.unsafeFromString(${lit(view.tableId.asString)}),
        §${level}partitionType = ${partitionType},
        §${level}query = ${genQuery(view.query.asString)},
        §${level}schema = ${genSchema(view.schema)},
        §${level}description = ${description(view.description)},
        §${level}labels = ${labels},
        §$outerLevel)""".stripMargin('§')
  }

  // todo: can we use pprint for this?
  def genTableDef(table: BQTableDef.Table[Any]) = {
    val partitionType = genPartitionType(table.partitionType)
    val labels =
      if (table.labels.values.isEmpty) "TableLabels.Empty"
      else table.labels.values.map { case (k, v) => s"${lit(k)} -> ${lit(v)}" }.mkString("TableLabels(", ", ", ")")
    val clustering = table.clustering.map(ident => s"Ident(${lit(ident.value)})").mkString("List(", ", ", ")")
    val options =
      if (table.tableOptions == TableOptions.Empty) "TableOptions.Empty"
      else s"TableOptions(partitionFilterRequired = ${table.tableOptions.partitionFilterRequired})"

    val outerLevel = repeat(2)(" ")
    val level = repeat(4)(" ")
    s"""|BQTableDef.Table(
        |${level}tableId = BQTableId.unsafeFromString(${lit(table.tableId.asString)}),
        |${level}schema = ${genSchema(table.schema)},
        |${level}partitionType = ${partitionType},
        |${level}description = ${description(table.description)},
        |${level}clustering = $clustering,
        |${level}labels = $labels,
        |${level}tableOptions = $options
        |$outerLevel)""".stripMargin
  }
}

object CodeGen {
  def generate(tables: immutable.Seq[BQTableDef[Any]], basePackage: List[String]) =
    tables.collect {
      case table: BQTableDef.Table[_] =>
        Source(SourceLocation(table.tableId, basePackage, "Table"), Generators.genTableDef(table))
      case view: BQTableDef.View[_] =>
        Source(SourceLocation(view.tableId, basePackage, "View"), Generators.genViewDef(view))
    }
}

case class Source(loc: SourceLocation, definition: String) {
  def asObject =
    s"""|package ${loc.pkgNames.mkString(".")}
        |
        |import no.nrk.bigquery._
        |import no.nrk.bigquery.syntax._
        |
        |object ${loc.typeName} {
        |  val definition = $definition
        |}
        |""".stripMargin

  def writeTo(basedir: Path) = {
    val destination = loc.destinationFile(basedir)
    Files.write(destination, asObject.getBytes(StandardCharsets.UTF_8))
    ()
  }
}

case class SourceLocation(pkgNames: List[String], typeName: String) {
  def destinationFile(basedir: Path): Path =
    basedir.resolve(s"src/main/scala/${pkgNames.mkString("/")}/$typeName.scala")
}

object SourceLocation {
  def apply(tableId: BQTableId, packages: List[String], suffix: String): SourceLocation = {
    def clean(str: String): String = {
      val camelCase = str
        .split("[-_ ]")
        .zipWithIndex
        .map {
          case (frag, 0) => frag
          case (frag, _) => frag.capitalize
        }
        .mkString

      camelCase.filter(_.isLetterOrDigit) match {
        case str if str.head.isDigit => "x" + str
        case str => str
      }
    }

    val pkgNames =
      packages ::: List(clean(tableId.dataset.project.value), clean(tableId.dataset.id))

    val objectName = clean(tableId.tableName).capitalize
    SourceLocation(pkgNames, objectName + suffix)
  }
}
