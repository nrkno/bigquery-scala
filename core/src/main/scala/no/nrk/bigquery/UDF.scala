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
import shapeless.{Nat, Sized, SizedBuilder, _0}

sealed trait UDF[+A <: UDFId, N <: Nat] {
  def name: A
  def params: Sized[Seq[UDF.Param], N]
  def returnType: Option[BQType]
  def apply(args: Sized[Seq[BQSqlFrag.Magnet], N]): BQSqlFrag.Call =
    BQSqlFrag.Call(this, args.unsized.toList.map(_.frag))
}
object UDF {

  type Params[N <: Nat] = Sized[IndexedSeq[Param], N]

  object Params {
    val empty: Sized[IndexedSeq[UDF.Param], _0] = Sized.wrap[IndexedSeq[UDF.Param], _0](IndexedSeq.empty[UDF.Param])
    def apply = new SizedBuilder[
      IndexedSeq
    ]() // todo: figure out why we need to explicit call apply. Might be related to the type alias
    def of = new SizedBuilder[IndexedSeq]()
  }

  case class Temporary[N <: Nat](
      name: TemporaryId,
      params: Params[N],
      body: UDF.Body,
      returnType: Option[BQType]
  ) extends UDF[UDFId.TemporaryId, N] {
    lazy val definition: BQSqlFrag = {
      val returning = returnType match {
        case Some(returnType) => bqfr" RETURNS $returnType"
        case None => BQSqlFrag.Empty
      }
      val language = body match {
        case _: Body.Sql => bqsql""
        case _: Body.Js => bqsql" LANGUAGE js"
      }

      bqfr"CREATE TEMP FUNCTION ${name}${params.unsized.map(_.definition).mkFragment("(", ", ", ")")}$returning${language} AS ${body.asFragment};"
    }
  }

  case class Persistent[N <: Nat](
      name: PersistentId,
      params: Params[N],
      body: UDF.Body,
      returnType: Option[BQType]
  ) extends UDF[UDFId.PersistentId, N] {
    def convertToTemporary: Temporary[N] =
      Temporary(TemporaryId(name.name), params, body, returnType)
  }

  case class Reference[N <: Nat](
      name: UDFId,
      params: Params[N],
      returnType: Option[BQType]
  ) extends UDF[UDFId, N]

  def temporary[N <: Nat](
      name: Ident,
      params: Params[N],
      body: UDF.Body,
      returnType: Option[BQType]
  ): Temporary[N] =
    Temporary(UDFId.TemporaryId(name), params, body, returnType)

  def persistent[N <: Nat](
      name: Ident,
      dataset: BQDataset,
      params: Params[N],
      body: UDF.Body,
      returnType: Option[BQType]
  ): Persistent[N] =
    Persistent(UDFId.PersistentId(dataset, name), params, body, returnType)

  def reference[N <: Nat](
      name: Ident,
      dataset: BQDataset,
      params: Params[N],
      returnType: Option[BQType]
  ): Reference[N] =
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
