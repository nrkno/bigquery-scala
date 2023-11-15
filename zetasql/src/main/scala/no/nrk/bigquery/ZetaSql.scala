/*
 * Copyright 2020 NRK
 *
 * SPDX-License-Identifier: MIT
 */

package no.nrk.bigquery

import cats.syntax.all.*
import cats.effect.Sync
import com.google.common.collect.ImmutableList
import com.google.zetasql.ZetaSQLFunctions.SignatureArgumentKind
import no.nrk.bigquery.syntax.*
import com.google.zetasql.{
  AnalyzerOptions,
  FunctionArgumentType,
  FunctionSignature,
  ParseLocationRange,
  Parser,
  SimpleColumn,
  SimpleTable,
  SqlException,
  StructType,
  TVFRelation,
  Type,
  TypeFactory
}
import com.google.zetasql.ZetaSQLType.TypeKind
import com.google.zetasql.resolvedast.ResolvedCreateStatementEnums.{CreateMode, CreateScope}
import com.google.zetasql.resolvedast.ResolvedNodes
import com.google.zetasql.toolkit.catalog.basic.BasicCatalogWrapper
import com.google.zetasql.toolkit.options.BigQueryLanguageOptions
import com.google.zetasql.parser.{ASTNodes, ParseTreeVisitor}
import com.google.zetasql.toolkit.catalog.TVFInfo
import com.google.zetasql.toolkit.{AnalysisException, AnalyzedStatement, ZetaSQLToolkitAnalyzer}
import no.nrk.bigquery.TVF.TVFId

import scala.collection.mutable.ListBuffer
import scala.collection.immutable
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

class ZetaSql[F[_]](implicit F: Sync[F]) {
  import ZetaSql._
  def parse(frag: BQSqlFrag): F[Either[SqlException, BQSqlFrag]] = parseScript(frag).map(_.as(frag))

  def parseScript(frag: BQSqlFrag): F[Either[SqlException, ASTNodes.ASTScript]] =
    F.interruptible {
      val options = BigQueryLanguageOptions.get()

      try
        Right(Parser.parseScript(frag.asString, options))
      catch {
        case e: SqlException => Left(e) // only catch sql exception and let all others bubble up to IO
      }
    }

  def parseAndBuildAnalysableFragment(
      query: String,
      allTables: immutable.Seq[BQTableLike[Any]],
      allTVF: immutable.Seq[TVF[Any, _]],
      toTableFragment: BQTableLike[Any] => BQSqlFrag = _.unpartitioned.bqShow,
      tableIdEqv: (BQTableId, BQTableId) => Boolean = _ == _,
      tvfIdEqv: (TVFId, TVFId) => Boolean = _ == _
  ): F[BQSqlFrag] = {
    sealed trait FragLoc {
      def loc: ParseLocationRange
    }
    object FragLoc {
      case class Table(id: BQTableId, loc: ParseLocationRange) extends FragLoc
      case class TVF(id: TVFId, argsLoc: List[ParseLocationRange], loc: ParseLocationRange) extends FragLoc
    }

    def evalFragments(
        parsedTables: List[FragLoc]
    ): BQSqlFrag = {
      val (rest, aggregate) = parsedTables
        .filter {
          case FragLoc.Table(id, _) => allTables.exists(t => tableIdEqv(id, t.tableId))
          case FragLoc.TVF(id, _, _) => allTVF.exists(t => tvfIdEqv(id, t.name))
        }
        .sortBy(_.loc.start())
        .foldLeft((0, Vector.empty[BQSqlFrag])) { case ((offset, agg), fragLoc) =>
          val prefix = BQSqlFrag.Frag(query.substring(offset, fragLoc.loc.start() - 1) + " ")
          val frag = fragLoc match {
            case FragLoc.Table(id, _) =>
              agg ++ allTables
                .find(t => tableIdEqv(id, t.tableId))
                .map(t => List(prefix, toTableFragment(t)))
                .getOrElse(Nil)
            case FragLoc.TVF(id, argsLoc, _) =>
              val tvfFrag = allTVF
                .find(t => tvfIdEqv(id, t.name))
                .map(tvf =>
                  List(
                    prefix,
                    BQSqlFrag.TableRef(BQAppliedTableValuedFunction[Any](
                      tvf.name,
                      tvf.partitionType,
                      tvf.params.unsized.toList,
                      tvf.query,
                      tvf.schema,
                      tvf.description,
                      argsLoc.map(l => BQSqlFrag(query.substring(l.start(), l.end())))
                    ))
                  ))
                .getOrElse(Nil)
              agg ++ tvfFrag

          }
          fragLoc.loc.end() -> frag
        }
      val str = query.substring(rest)

      BQSqlFrag.Combined(if (str.nonEmpty) aggregate :+ BQSqlFrag.Frag(str) else aggregate)
    }

    parseScript(BQSqlFrag.Frag(query))
      .flatMap(F.fromEither)
      .flatMap { script =>
        val list = script.getStatementListNode.getStatementList
        if (list.size() != 1) {
          F.raiseError[List[FragLoc]](new IllegalArgumentException("Expects only one statement"))
        } else
          F.delay {
            val buffer = new ListBuffer[FragLoc]
            list.asScala.headOption.foreach { rootNode =>
              rootNode.accept(new ParseTreeVisitor {
                override def visit(node: ASTNodes.ASTTablePathExpression): Unit =
                  node.getPathExpr.getNames.forEach(ident =>
                    BQTableId
                      .fromString(ident.getIdString)
                      .toOption
                      .foreach(id => buffer += FragLoc.Table(id, ident.getParseLocationRange)))

                override def visit(node: ASTNodes.ASTTVF): Unit =
                  node.getName.getNames.forEach { ident =>
                    val argLoc = new ListBuffer[ParseLocationRange]
                    node.getArgumentEntries.asScala.foreach(_.accept(new ParseTreeVisitor {
                      override def visit(node: ASTNodes.ASTTVFArgument): Unit =
                        argLoc += node.getParseLocationRange
                    }))
                    BQTableId
                      .fromString(ident.getIdString)
                      .toOption
                      .foreach(id => buffer += FragLoc.TVF(TVFId(id), argLoc.toList, node.getParseLocationRange))
                  }
              })
            }
            buffer.toList
          }
      }
      .map(evalFragments)
  }

  def queryFields(frag: BQSqlFrag): F[List[BQField]] =
    analyzeFirst(frag).map { res =>
      val builder = List.newBuilder[BQField]

      res
        .flatMap(_.getResolvedStatement.toScala.toRight(new AnalysisException("No analysis found")))
        .foreach(tree =>
          tree.accept(new ResolvedNodes.Visitor {
            override def visit(node: ResolvedNodes.ResolvedQueryStmt): Unit =
              node.getOutputColumnList.asScala.foreach(col =>
                builder += fromColumnNameAndType(col.getColumn.getName, col.getColumn.getType))
          }))
      builder.result()
    }

  def analyzeFirst(frag: BQSqlFrag): F[Either[AnalysisException, AnalyzedStatement]] =
    F.interruptible {
      val tables = frag.allReferencedTables
      val catalog = toCatalog(tables: _*)
      val rendered = frag.asString

      val options = BigQueryLanguageOptions.get()
      val analyzerOptions = new AnalyzerOptions
      analyzerOptions.setLanguageOptions(options)
      analyzerOptions.setPreserveColumnAliases(true)

      val analyser = new ZetaSQLToolkitAnalyzer(analyzerOptions)
      val analyzed = analyser.analyzeStatements(rendered, catalog)

      if (analyzed.hasNext)
        Right(analyzed.next())
      else Left(new AnalysisException("Unable to find any analyzed statements"))
    }
}

object ZetaSql {
  def apply[F[_]: Sync]: ZetaSql[F] = new ZetaSql[F]

  def toCatalog(tables: BQTableLike[Any]*): BasicCatalogWrapper = {
    val catalog = new BasicCatalogWrapper()
    tables.foreach {
      case tableDef: BQTableDef[_] =>
        catalog.register(
          toSimpleTable(tableDef.tableId, Some(tableDef.schema)),
          CreateMode.CREATE_IF_NOT_EXISTS,
          CreateScope.CREATE_DEFAULT_SCOPE)
      case tableRef: BQTableRef[_] =>
        catalog.register(
          toSimpleTable(tableRef.tableId, None),
          CreateMode.CREATE_IF_NOT_EXISTS,
          CreateScope.CREATE_DEFAULT_SCOPE)
      case atvf: BQAppliedTableValuedFunction[Any] =>
        catalog.register(
          toTableValuedFunction(atvf),
          CreateMode.CREATE_IF_NOT_EXISTS,
          CreateScope.CREATE_DEFAULT_SCOPE
        )
    }

    catalog
  }

  def fromColumnNameAndType(name: String, typ: Type): BQField =
    if (typ.isArray) {
      val elem = fromColumnNameAndType(name, typ.asArray().getElementType)
      elem.copy(mode = BQField.Mode.REPEATED)
    } else if (typ.isStruct) {
      BQField.struct(name, BQField.Mode.NULLABLE)(
        typ
          .asStruct()
          .getFieldList
          .asScala
          .map(subField => fromColumnNameAndType(subField.getName, subField.getType))
          .toList: _*)
    } else {
      val kind = typ.getKind match {
        case TypeKind.TYPE_BOOL => BQField.Type.BOOL
        case TypeKind.TYPE_DATE => BQField.Type.DATE
        case TypeKind.TYPE_DATETIME => BQField.Type.DATETIME
        case TypeKind.TYPE_JSON => BQField.Type.JSON
        case TypeKind.TYPE_BYTES => BQField.Type.BYTES
        case TypeKind.TYPE_STRING => BQField.Type.STRING
        case TypeKind.TYPE_BIGNUMERIC => BQField.Type.BIGNUMERIC
        case TypeKind.TYPE_INT64 => BQField.Type.INT64
        case TypeKind.TYPE_INT32 => BQField.Type.INT64
        case TypeKind.TYPE_FLOAT => BQField.Type.FLOAT64
        case TypeKind.TYPE_DOUBLE => BQField.Type.FLOAT64
        case TypeKind.TYPE_TIMESTAMP => BQField.Type.TIMESTAMP
        case TypeKind.TYPE_TIME => BQField.Type.TIME
        case TypeKind.TYPE_GEOGRAPHY => BQField.Type.GEOGRAPHY
        case TypeKind.TYPE_INTERVAL => BQField.Type.INTERVAL
        case _ => throw new IllegalArgumentException(s"$name with type ${typ.debugString()} is not supported ")
      }
      BQField(name, kind, BQField.Mode.NULLABLE)
    }

  private def toType(bqType: BQType): Type = {
    val isArray = bqType.mode == BQField.Mode.REPEATED
    val elemType = bqType.tpe match {
      case BQField.Type.BOOL => TypeFactory.createSimpleType(TypeKind.TYPE_BOOL)
      case BQField.Type.INT64 => TypeFactory.createSimpleType(TypeKind.TYPE_INT64)
      case BQField.Type.FLOAT64 => TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT)
      case BQField.Type.NUMERIC => TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC)
      case BQField.Type.BIGNUMERIC => TypeFactory.createSimpleType(TypeKind.TYPE_BIGNUMERIC)
      case BQField.Type.STRING => TypeFactory.createSimpleType(TypeKind.TYPE_STRING)
      case BQField.Type.BYTES => TypeFactory.createSimpleType(TypeKind.TYPE_BYTES)
      case BQField.Type.STRUCT =>
        TypeFactory.createStructType(
          bqType.subFields.map(f => new StructType.StructField(f._1, toType(f._2))).asJavaCollection
        )
      case BQField.Type.ARRAY => TypeFactory.createArrayType(toType(bqType.subFields.head._2))
      case BQField.Type.TIMESTAMP => TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP)
      case BQField.Type.DATE => TypeFactory.createSimpleType(TypeKind.TYPE_DATE)
      case BQField.Type.TIME => TypeFactory.createSimpleType(TypeKind.TYPE_TIME)
      case BQField.Type.DATETIME => TypeFactory.createSimpleType(TypeKind.TYPE_DATETIME)
      case BQField.Type.GEOGRAPHY => TypeFactory.createSimpleType(TypeKind.TYPE_GEOGRAPHY)
      case BQField.Type.JSON => TypeFactory.createSimpleType(TypeKind.TYPE_JSON)
      case BQField.Type.INTERVAL => TypeFactory.createSimpleType(TypeKind.TYPE_INTERVAL)
    }
    if (isArray) TypeFactory.createArrayType(elemType) else elemType
  }

  def toTableValuedFunction(atvf: BQAppliedTableValuedFunction[Any]): TVFInfo = {
    val tableType = new FunctionArgumentType(SignatureArgumentKind.ARG_TYPE_RELATION)
    val args = new java.util.ArrayList(
      atvf.params
        .map(_.maybeType.map(tpe => toType(tpe)).getOrElse(TypeFactory.createSimpleType(TypeKind.TYPE_UNKNOWN)))
        .map(new FunctionArgumentType(_))
        .asJavaCollection)

    TVFInfo
      .newBuilder()
      .setSignature(new FunctionSignature(tableType, args, -1))
      .setOutputSchema(
        TVFRelation.createColumnBased(
          new java.util.ArrayList(atvf.schema.fields
            .map(f => TVFRelation.Column.create(f.name, toType(BQType.fromField(f))))
            .asJavaCollection)
        ))
      .setNamePath(ImmutableList.of(atvf.name.asString))
      .build()
  }

  def toSimpleTable(tableId: BQTableId, schema: => Option[BQSchema]): SimpleTable = {

    def toSimpleField(field: BQField) =
      new SimpleColumn(tableId.tableName, field.name, toType(BQType.fromField(field)), false, true)

    val simple = schema match {
      case None =>
        new SimpleTable(tableId.tableName)
      case Some(s) =>
        new SimpleTable(
          tableId.tableName,
          new java.util.ArrayList(s.fields.map(toSimpleField).asJavaCollection)
        )
    }
    simple.setFullName(tableId.asString)
    simple
  }
}
