/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package no.nrk.bigquery

import cats.data.NonEmptyList
import cats.syntax.all._
import cats.Show
import no.nrk.bigquery.UDF._
import no.nrk.bigquery.UDF.UDFId._
import no.nrk.bigquery.syntax._

sealed trait UDF[+A <: UDFId] {
  def name: A
  def params: List[UDF.Param]
  def returnType: Option[BQType]
  def apply(args: BQSqlFrag.Magnet*): BQSqlFrag.Call =
    BQSqlFrag.Call(this, args.toList.map(_.frag))
}
object UDF {

  case class Temporary(
      name: TemporaryId,
      params: List[UDF.Param],
      body: UDF.Body,
      returnType: Option[BQType]
  ) extends UDF[UDFId.TemporaryId] {
    lazy val definition: BQSqlFrag = {
      val returning = returnType match {
        case Some(returnType) => bqfr" RETURNS $returnType"
        case None => BQSqlFrag.Empty
      }
      val language = body match {
        case _: Body.Sql => bqsql""
        case _: Body.Js => bqsql" LANGUAGE js"
      }

      bqfr"CREATE TEMP FUNCTION ${name}${params.map(_.definition).mkFragment("(", ", ", ")")}$returning${language} AS ${body.asFragment};"
    }
  }

  case class Persistent(
      name: PersistentId,
      params: List[UDF.Param],
      body: UDF.Body,
      returnType: Option[BQType]
  ) extends UDF[UDFId.PersistentId] {
    def convertToTemporary: Temporary =
      Temporary(TemporaryId(name.name), params, body, returnType)
  }

  case class Reference(
      name: UDFId,
      params: List[UDF.Param],
      returnType: Option[BQType]
  ) extends UDF[UDFId]

  @deprecated("use UDF.temporary constructor", "0.5")
  def apply(
      name: Ident,
      params: Seq[UDF.Param],
      body: BQSqlFrag,
      returnType: Option[BQType]
  ): Temporary =
    Temporary(UDFId.TemporaryId(name), params.toList, UDF.Body.Sql(body), returnType)

  def temporary(
      name: Ident,
      params: List[UDF.Param],
      body: UDF.Body,
      returnType: Option[BQType]
  ): Temporary =
    Temporary(UDFId.TemporaryId(name), params, body, returnType)

  def persistent(
      name: Ident,
      dataset: BQDataset,
      params: List[UDF.Param],
      body: UDF.Body,
      returnType: Option[BQType]
  ): Persistent =
    Persistent(UDFId.PersistentId(dataset, name), params, body, returnType)

  def reference(
      name: Ident,
      dataset: BQDataset,
      params: List[UDF.Param],
      returnType: Option[BQType]
  ): Reference =
    Reference(UDFId.PersistentId(dataset, name), params, returnType)

  sealed trait UDFId {
    def asString: String
    def asFragment: BQSqlFrag
  }

  object UDFId {
    case class TemporaryId(name: Ident) extends UDFId {
      override def asString: String = name.value
      override def asFragment: BQSqlFrag = name.bqShow
    }

    object TemporaryId {
      implicit val bqShows: BQShow[TemporaryId] = _.asFragment
    }

    case class PersistentId(dataset: BQDataset, name: Ident) extends UDFId {
      override def asString: String = show"${dataset.project.value}.${dataset.id}.$name"
      override def asFragment: BQSqlFrag = BQSqlFrag.backticks(asString)
    }

    object PersistentId {
      implicit val bqShow: BQShow[PersistentId] = _.asFragment
    }

    implicit val bqShow: BQShow[UDFId] = _.asFragment
    implicit val show: Show[UDFId] = _.asString
  }

  case class Param(name: Ident, maybeType: Option[BQType]) {
    def definition: BQSqlFrag =
      maybeType match {
        case Some(tpe) => bqfr"$name $tpe"
        case None => bqfr"$name ANY TYPE"
      }
  }
  object Param {
    def apply(name: String, tpe: BQType): Param =
      Param(Ident(name), Some(tpe))

    def untyped(name: String): Param =
      Param(Ident(name), None)

    def fromField(field: BQField): Param =
      Param(Ident(field.name), Some(BQType.fromField(field)))
  }

  sealed trait Body {
    def asFragment: BQSqlFrag
  }
  object Body {
    case class Sql(body: BQSqlFrag) extends Body {
      override val asFragment: BQSqlFrag = bqfr"($body)"
    }
    case class Js(javascriptSnippet: String, gsLibraryPath: List[String]) extends Body {
      override def asFragment: BQSqlFrag = {
        val jsBody =
          bqfr"""|'''
                 |${BQSqlFrag(javascriptSnippet)}
                 |'''""".stripMargin

        NonEmptyList.fromList(gsLibraryPath) match {
          case None => jsBody
          case Some(libs) =>
            val paths = libs
              .map(lib => BQSqlFrag(if (!lib.startsWith("gs://")) show""""gs://$lib"""" else show""""$lib""""))
              .mkFragment("[", ",", "]")
            val libraryOption = bqfr"""OPTIONS ( library=$paths )"""
            bqfr"""|$jsBody
                   |$libraryOption""".stripMargin
        }
      }
    }
  }

}
