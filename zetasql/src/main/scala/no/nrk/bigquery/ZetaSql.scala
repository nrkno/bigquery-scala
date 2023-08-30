package no.nrk.bigquery

import cats.syntax.all._
import cats.effect.Sync
import no.nrk.bigquery.syntax._
import com.google.zetasql.{
  AnalyzerOptions,
  ParseLocationRange,
  Parser,
  SimpleColumn,
  SimpleTable,
  SqlException,
  StructType,
  Type,
  TypeFactory
}
import com.google.zetasql.ZetaSQLType.TypeKind
import com.google.zetasql.resolvedast.ResolvedCreateStatementEnums.{CreateMode, CreateScope}
import com.google.zetasql.resolvedast.ResolvedNodes
import com.google.zetasql.toolkit.catalog.basic.BasicCatalogWrapper
import com.google.zetasql.toolkit.options.BigQueryLanguageOptions
import com.google.zetasql.parser.{ASTNodes, ParseTreeVisitor}
import com.google.zetasql.toolkit.{AnalysisException, AnalyzedStatement, ZetaSQLToolkitAnalyzer}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

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
      allTables: List[BQTableLike[Any]],
      toFragment: BQTableLike[Any] => BQSqlFrag = _.unpartitioned.bqShow,
      eqv: (BQTableId, BQTableId) => Boolean = _ == _): F[BQSqlFrag] = {

    def evalFragments(
        parsedTables: List[(BQTableId, ParseLocationRange)]
    ): BQSqlFrag = {
      val asString = query
      val found = allTables
        .flatMap(table =>
          parsedTables.flatMap { case (id, range) => if (eqv(table.tableId, id)) List(table -> range) else Nil })
        .distinct
      val (rest, aggregate) = found.foldLeft((asString, BQSqlFrag.Empty)) { case ((input, agg), (t, loc)) =>
        val frag = agg ++ BQSqlFrag.Frag(input.substring(0, loc.start() - 1)) ++ toFragment(t)
        val rest = input.substring(loc.end())
        rest -> frag
      }

      aggregate ++ BQSqlFrag.Frag(rest)
    }

    parseScript(BQSqlFrag.Frag(query))
      .flatMap(F.fromEither)
      .flatMap { script =>
        val list = script.getStatementListNode.getStatementList
        if (list.size() != 1) {
          Sync[F].raiseError[List[(no.nrk.bigquery.BQTableId, com.google.zetasql.ParseLocationRange)]](
            new IllegalArgumentException("Expects only one statement"))
        } else
          Sync[F].delay {
            val buffer = new ListBuffer[(BQTableId, ParseLocationRange)]
            list.asScala.headOption.foreach(_.accept(new ParseTreeVisitor {
              override def visit(node: ASTNodes.ASTTablePathExpression): Unit =
                node.getPathExpr.getNames.forEach(ident =>
                  BQTableId
                    .fromString(ident.getIdString)
                    .toOption
                    .foreach(id => buffer += (id -> ident.getParseLocationRange)))
            }))
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
    tables.foreach(table =>
      catalog.register(toSimpleTable(table), CreateMode.CREATE_IF_NOT_EXISTS, CreateScope.CREATE_DEFAULT_SCOPE))
    catalog
  }

  def fromColumnNameAndType(name: String, typ: Type): BQField = {
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
    } else BQField(name, kind, BQField.Mode.NULLABLE)
  }

  def toSimpleTable(table: BQTableLike[Any]): SimpleTable = {
    def toType(field: BQField): Type = {
      val isArray = field.mode == BQField.Mode.REPEATED

      val elemType = field.tpe match {
        case BQField.Type.BOOL => TypeFactory.createSimpleType(TypeKind.TYPE_BOOL)
        case BQField.Type.INT64 => TypeFactory.createSimpleType(TypeKind.TYPE_INT64)
        case BQField.Type.FLOAT64 => TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT)
        case BQField.Type.NUMERIC => TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC)
        case BQField.Type.BIGNUMERIC => TypeFactory.createSimpleType(TypeKind.TYPE_BIGNUMERIC)
        case BQField.Type.STRING => TypeFactory.createSimpleType(TypeKind.TYPE_STRING)
        case BQField.Type.BYTES => TypeFactory.createSimpleType(TypeKind.TYPE_BYTES)
        case BQField.Type.STRUCT =>
          TypeFactory.createStructType(
            field.subFields
              .map(sub => new StructType.StructField(sub.name, toType(sub)))
              .asJavaCollection
          )
        case BQField.Type.ARRAY =>
          TypeFactory.createArrayType(toType(field.subFields.head))
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

    def toSimpleField(field: BQField) =
      new SimpleColumn(table.tableId.tableName, field.name, toType(field), false, true)

    val simple = table match {
      case BQTableRef(tableId, _, _) =>
        new SimpleTable(tableId.tableName)

      case tbl: BQTableDef.Table[_] =>
        new SimpleTable(
          tbl.tableId.tableName,
          new java.util.ArrayList(tbl.schema.fields.map(toSimpleField).asJavaCollection)
        )
      case view: BQTableDef.ViewLike[_] =>
        new SimpleTable(
          view.tableId.tableName,
          new java.util.ArrayList(view.schema.fields.map(toSimpleField).asJavaCollection)
        )
    }
    simple.setFullName(table.tableId.asString)
    simple
  }
}
