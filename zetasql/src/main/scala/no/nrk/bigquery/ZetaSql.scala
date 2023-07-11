package no.nrk.bigquery

import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.{Field, StandardSQLTypeName}
import com.google.zetasql.ZetaSQLType.TypeKind
import com.google.zetasql.resolvedast.ResolvedCreateStatementEnums.{CreateMode, CreateScope}
import com.google.zetasql.resolvedast.ResolvedNodes
import com.google.zetasql.toolkit.catalog.basic.BasicCatalogWrapper
import com.google.zetasql.{AnalyzerOptions, SimpleColumn, SimpleTable, StructType, Type, TypeFactory, toolkit}

import scala.jdk.CollectionConverters._

object ZetaSql {
  def queryFields(frag: BQSqlFrag): List[BQField] = {
    val builder = List.newBuilder[BQField]
    analyze(frag).foreach(tree =>
      tree.accept(new ResolvedNodes.Visitor {
        override def visit(node: ResolvedNodes.ResolvedQueryStmt): Unit =
          node.getOutputColumnList.asScala.foreach(col =>
            builder += ZetaSql.fromColumnNameAndType(col.getColumn.getName, col.getColumn.getType))
      }))
    builder.result()
  }

  def analyze(frag: BQSqlFrag): Option[ResolvedNodes.ResolvedStatement] = {
    val tables = frag.allReferencedTables
    val catalog = toCatalog(tables: _*)
    val rendered = frag.asString

    val options = toolkit.options.BigQueryLanguageOptions.get()
    val analyzerOptions = new AnalyzerOptions
    analyzerOptions.setLanguageOptions(options)
    analyzerOptions.setPreserveColumnAliases(true)

    val analyser = new toolkit.ZetaSQLToolkitAnalyzer(analyzerOptions)
    val analyzed = analyser.analyzeStatements(rendered, catalog)

    analyzed.asScala.nextOption()
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
      case _ => ???
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
        case StandardSQLTypeName.ARRAY => ???
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
