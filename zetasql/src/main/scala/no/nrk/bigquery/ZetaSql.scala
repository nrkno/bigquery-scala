package no.nrk.bigquery

import cats.syntax.all._
import cats.effect.IO
import no.nrk.bigquery.syntax._
import com.google.cloud.bigquery.{Field, StandardSQLTypeName}
import com.google.zetasql.ZetaSQLType.TypeKind
import com.google.zetasql.resolvedast.ResolvedCreateStatementEnums.{CreateMode, CreateScope}
import com.google.zetasql.resolvedast.ResolvedNodes
import com.google.zetasql.toolkit.catalog.basic.BasicCatalogWrapper
import com.google.zetasql.parser.{ASTNodes, ParseTreeVisitor}
import com.google.zetasql._

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object ZetaSql {
  def parse(frag: BQSqlFrag): IO[Either[SqlException, BQSqlFrag]] = parseScript(frag).map(_.as(frag))

  def parseScript(frag: BQSqlFrag): IO[Either[SqlException, parser.ASTNodes.ASTScript]] = IO.interruptible {
    val options = toolkit.options.BigQueryLanguageOptions.get()

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
      eqv: (BQTableId, BQTableId) => Boolean = _ == _): IO[BQSqlFrag] = {

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

    ZetaSql
      .parseScript(BQSqlFrag.Frag(query))
      .flatMap(IO.fromEither)
      .map { script =>
        val buffer = new ListBuffer[(BQTableId, ParseLocationRange)]
        script.getStatementListNode.getStatementList
          .get(0)
          .accept(new ParseTreeVisitor {
            override def visit(node: ASTNodes.ASTTablePathExpression): Unit =
              node.getPathExpr.getNames.forEach(ident =>
                BQTableId
                  .fromString(ident.getIdString)
                  .toOption
                  .foreach(id => buffer += (id -> ident.getParseLocationRange)))
          })
        buffer.toList
      }
      .map(evalFragments)
  }

  def queryFields(frag: BQSqlFrag): IO[List[BQField]] =
    analyzeFirst(frag).map { res =>
      val builder = List.newBuilder[BQField]

      res.foreach(tree =>
        tree.accept(new ResolvedNodes.Visitor {
          override def visit(node: ResolvedNodes.ResolvedQueryStmt): Unit =
            node.getOutputColumnList.asScala.foreach(col =>
              builder += ZetaSql.fromColumnNameAndType(col.getColumn.getName, col.getColumn.getType))
        }))
      builder.result()
    }

  def analyzeFirst(frag: BQSqlFrag): IO[Option[ResolvedNodes.ResolvedStatement]] = IO.interruptible {
    val tables = frag.allReferencedTables
    val catalog = toCatalog(tables: _*)
    val rendered = frag.asString

    val options = toolkit.options.BigQueryLanguageOptions.get()
    val analyzerOptions = new AnalyzerOptions
    analyzerOptions.setLanguageOptions(options)
    analyzerOptions.setPreserveColumnAliases(true)

    val analyser = new toolkit.ZetaSQLToolkitAnalyzer(analyzerOptions)
    val analyzed = analyser.analyzeStatements(rendered, catalog)

    if (analyzed.hasNext) Option(analyzed.next()) else None
  }

  def toCatalog(tables: BQTableLike[Any]*): BasicCatalogWrapper = {
    val catalog = new BasicCatalogWrapper()
    tables.foreach(table =>
      catalog.register(toSimpleTable(table), CreateMode.CREATE_IF_NOT_EXISTS, CreateScope.CREATE_DEFAULT_SCOPE))
    catalog
  }

  def fromColumnNameAndType(name: String, typ: Type): BQField = {
    val kind = typ.getKind match {
      case TypeKind.TYPE_BOOL => StandardSQLTypeName.BOOL
      case TypeKind.TYPE_DATE => StandardSQLTypeName.DATE
      case TypeKind.TYPE_DATETIME => StandardSQLTypeName.DATETIME
      case TypeKind.TYPE_JSON => StandardSQLTypeName.JSON
      case TypeKind.TYPE_BYTES => StandardSQLTypeName.BYTES
      case TypeKind.TYPE_STRING => StandardSQLTypeName.STRING
      case TypeKind.TYPE_BIGNUMERIC => StandardSQLTypeName.BIGNUMERIC
      case TypeKind.TYPE_INT64 => StandardSQLTypeName.INT64
      case TypeKind.TYPE_INT32 => StandardSQLTypeName.INT64
      case TypeKind.TYPE_FLOAT => StandardSQLTypeName.FLOAT64
      case TypeKind.TYPE_DOUBLE => StandardSQLTypeName.FLOAT64
      case TypeKind.TYPE_TIMESTAMP => StandardSQLTypeName.TIMESTAMP
      case TypeKind.TYPE_TIME => StandardSQLTypeName.TIME
      case TypeKind.TYPE_GEOGRAPHY => StandardSQLTypeName.GEOGRAPHY
      case TypeKind.TYPE_INTERVAL => StandardSQLTypeName.INTERVAL
      case _ => throw new IllegalArgumentException(s"$name with type ${typ.debugString()} is not supported ")
    }

    if (typ.isArray) {
      val elem = fromColumnNameAndType(name, typ.asArray().getElementType)
      elem.copy(mode = Field.Mode.REPEATED)
    } else if (typ.isStruct) {
      BQField.struct(name, Field.Mode.NULLABLE)(
        typ
          .asStruct()
          .getFieldList
          .asScala
          .map(subField => fromColumnNameAndType(subField.getName, subField.getType))
          .toList: _*)
    } else BQField(name, kind, Field.Mode.NULLABLE)
  }

  def toSimpleTable(table: BQTableLike[Any]): SimpleTable = {
    def toType(field: BQField): Type = {
      val isArray = field.mode == Field.Mode.REPEATED

      val elemType = field.tpe match {
        case StandardSQLTypeName.BOOL => TypeFactory.createSimpleType(TypeKind.TYPE_BOOL)
        case StandardSQLTypeName.INT64 => TypeFactory.createSimpleType(TypeKind.TYPE_INT64)
        case StandardSQLTypeName.FLOAT64 => TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT)
        case StandardSQLTypeName.NUMERIC => TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC)
        case StandardSQLTypeName.BIGNUMERIC => TypeFactory.createSimpleType(TypeKind.TYPE_BIGNUMERIC)
        case StandardSQLTypeName.STRING => TypeFactory.createSimpleType(TypeKind.TYPE_STRING)
        case StandardSQLTypeName.BYTES => TypeFactory.createSimpleType(TypeKind.TYPE_BYTES)
        case StandardSQLTypeName.STRUCT =>
          TypeFactory.createStructType(
            field.subFields
              .map(sub => new StructType.StructField(sub.name, toType(sub)))
              .asJavaCollection
          )
        case StandardSQLTypeName.ARRAY =>
          throw new IllegalArgumentException(s"${field.name} with type ARRAY is not supported")
        case StandardSQLTypeName.TIMESTAMP => TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP)
        case StandardSQLTypeName.DATE => TypeFactory.createSimpleType(TypeKind.TYPE_DATE)
        case StandardSQLTypeName.TIME => TypeFactory.createSimpleType(TypeKind.TYPE_TIME)
        case StandardSQLTypeName.DATETIME => TypeFactory.createSimpleType(TypeKind.TYPE_DATETIME)
        case StandardSQLTypeName.GEOGRAPHY => TypeFactory.createSimpleType(TypeKind.TYPE_GEOGRAPHY)
        case StandardSQLTypeName.JSON => TypeFactory.createSimpleType(TypeKind.TYPE_JSON)
        case StandardSQLTypeName.INTERVAL => TypeFactory.createSimpleType(TypeKind.TYPE_INTERVAL)
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
