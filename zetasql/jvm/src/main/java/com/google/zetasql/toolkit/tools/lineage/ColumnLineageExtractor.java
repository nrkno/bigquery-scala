/*
 * Copyright 2023 Google LLC All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.zetasql.toolkit.tools.lineage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.zetasql.Table;
import com.google.zetasql.Type;
import com.google.zetasql.resolvedast.ResolvedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedQueryStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedColumnRef;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedCreateTableAsSelectStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedCreateViewBase;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedGetStructField;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedInsertRow;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedInsertStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedMergeStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedMergeWhen;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedOutputColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedUpdateItem;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedUpdateStmt;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Implements extraction of column-level lineage from ZetaSQL {@link ResolvedStatement}s.
 * Supported statements:
 * <ul>
 *   <li> SELECT
 *   <li> CREATE TABLE AS SELECT
 *   <li> CREATE [MATERIALIZED] VIEW AS SELECT
 *   <li> INSERT
 *   <li> UPDATE
 *   <li> MERGE
 * </ul>
 */
public class ColumnLineageExtractor {

  private static ColumnLineage buildColumnLineage(
      String targetTableName, String targetColumnName, Collection<ResolvedColumn> parentColumns) {
    ColumnEntity target = new ColumnEntity(targetTableName, targetColumnName);
    Set<ColumnEntity> parents = parentColumns
        .stream()
        .map(ColumnEntity::forResolvedColumn)
        .collect(Collectors.toSet());
    return new ColumnLineage(target, parents);
  }

  // TODO: expandColumns() is also defined in ParentColumnFinder. Generalize.
  private static List<ResolvedColumn> expandColumn(ResolvedColumn column) {
    Type type = column.getType();

    if (type.isStruct()) {
      return type.asStruct().getFieldList()
          .stream()
          .map(field -> buildColumnSubfield(column, field.getName(), field.getType()))
          .map(ColumnLineageExtractor::expandColumn)
          .flatMap(List::stream)
          .collect(Collectors.toList());
    }

    return ImmutableList.of(column);
  }

  /**
   * Extracts the column-level lineage entries for a set of {@link ResolvedOutputColumn}s,
   * given the {@link ResolvedStatement} they belong to.
   *
   * @param targetTableName The name of the table the output columns write to
   * @param outputColumns The output columns to find lineage for
   * @param statement The ResolvedStatement the output columns belong to
   * @return The set of resulting {@link ColumnLineage} objects
   */
  private static Set<ColumnLineage> extractColumnLevelLineageForOutputColumns(
      String targetTableName,
      List<ResolvedOutputColumn> outputColumns,
      ResolvedStatement statement) {

    HashSet<ColumnLineage> result = new HashSet<>();

    for (ResolvedOutputColumn outputColumn : outputColumns) {
      List<ResolvedColumn> expandedResolvedColumns = expandColumn(outputColumn.getColumn());

      for (ResolvedColumn expandedResolvedColumn : expandedResolvedColumns) {
        List<ResolvedColumn> parentColumns =
            ParentColumnFinder.findParentsForColumn(statement, expandedResolvedColumn);
        ColumnLineage lineageEntry =
            buildColumnLineage(targetTableName, outputColumn.getName(), parentColumns);
        result.add(lineageEntry);
      }
    }

    return result;

  }

  /**
   * Extracts the column-level lineage entries for a {@link ResolvedCreateTableAsSelectStmt}
   *
   * @param createTableAsSelectStmt The ResolvedCreateTableAsSelectStmt for which to extract lineage
   * @return The set of resulting {@link ColumnLineage} objects
   */
  public static Set<ColumnLineage> extractColumnLevelLineage(
      ResolvedCreateTableAsSelectStmt createTableAsSelectStmt) {

    String fullTableName = String.join(".", createTableAsSelectStmt.getNamePath());

    List<ResolvedOutputColumn> outputColumns = createTableAsSelectStmt.getOutputColumnList();

    return extractColumnLevelLineageForOutputColumns(
        fullTableName, outputColumns, createTableAsSelectStmt);
  }

  /**
   * Extracts the column-level lineage entries for a {@link ResolvedCreateViewBase} statement
   *
   * @param createViewBase The ResolvedCreateViewBase statement for which to extract lineage
   * @return The set of resulting {@link ColumnLineage} objects
   */
  public static Set<ColumnLineage> extractColumnLevelLineage(
      ResolvedCreateViewBase createViewBase) {
    String fullViewName = String.join(".", createViewBase.getNamePath());

    List<ResolvedOutputColumn> outputColumns = createViewBase.getOutputColumnList();

    return extractColumnLevelLineageForOutputColumns(
        fullViewName, outputColumns, createViewBase);
  }

  /**
   * Extracts the column-level lineage entries for a {@link ResolvedQueryStmt} statement
   *
   * @param statement The ResolvedQueryStmt statement for which to extract lineage
   * @param outputTable The name of the table the statement write to
   * @return The set of resulting {@link ColumnLineage} objects
   */
  public static Set<ColumnLineage> extractColumnLevelLineage(
      ResolvedQueryStmt statement, String outputTable) {
    List<ResolvedOutputColumn> outputColumns = statement.getOutputColumnList();
    return extractColumnLevelLineageForOutputColumns(outputTable, outputColumns, statement);
  }

  /**
   * Extracts the column-level lineage entries for a {@link ResolvedInsertStmt}
   *
   * @param insertStmt The ResolvedInsertStmt for which to extract lineage
   * @return The set of resulting {@link ColumnLineage} objects
   */
  public static Set<ColumnLineage> extractColumnLevelLineage(ResolvedInsertStmt insertStmt) {
    if (Objects.isNull(insertStmt.getQuery())) {
      // The statement is inserting rows manually using "INSERT INTO ... VALUES ..."
      // Since it does not query any tables, it does not produce lineage
      return ImmutableSet.of();
    }

    Table targetTable = insertStmt.getTableScan().getTable();
    ResolvedScan query = insertStmt.getQuery();
    List<ResolvedColumn> insertedColumns = insertStmt.getInsertColumnList()
        .stream()
        .map(ColumnLineageExtractor::expandColumn)
        .flatMap(List::stream)
        .collect(Collectors.toList());
    List<ResolvedColumn> matchingColumnsInQuery = query.getColumnList()
        .stream()
        .map(ColumnLineageExtractor::expandColumn)
        .flatMap(List::stream)
        .collect(Collectors.toList());

    return IntStream.range(0, insertedColumns.size())
        .mapToObj(index -> new SimpleEntry<>(
            insertedColumns.get(index),
            ParentColumnFinder.findParentsForColumn(insertStmt, matchingColumnsInQuery.get(index))))
        .map(entry -> buildColumnLineage(
            targetTable.getFullName(), entry.getKey().getName(), entry.getValue()))
        .collect(Collectors.toSet());
  }

  // TODO: buildColumnSubfield() is also defined in ParentColumnFinder. Generalize.
  private static ResolvedColumn buildColumnSubfield(
      ResolvedColumn column, String fieldName, Type fieldType) {
    return new ResolvedColumn(
        column.getId(),
        column.getTableName(),
        column.getName() + "." + fieldName,
        fieldType);
  }

  private static Optional<ResolvedColumn> resolveUpdateItemTarget(ResolvedExpr updateTarget) {
    if (updateTarget instanceof ResolvedColumnRef) {
      ResolvedColumnRef columnRef = (ResolvedColumnRef) updateTarget;
      return Optional.of(columnRef.getColumn());
    } else if (updateTarget instanceof ResolvedGetStructField) {
      ResolvedGetStructField getStructField = (ResolvedGetStructField) updateTarget;
      int structFieldIdx = (int) getStructField.getFieldIdx();
      String fieldName = getStructField.getExpr()
          .getType()
          .asStruct()
          .getField(structFieldIdx)
          .getName();
      return resolveUpdateItemTarget(getStructField.getExpr())
          .map(target -> buildColumnSubfield(target, fieldName, getStructField.getType()));
    }

    return Optional.empty();
  }

  /**
   * Extracts the column-level lineage entry for a {@link ResolvedUpdateItem}. ResolvedUpdateItems
   * represent a "SET column = expression" clause and are used in UPDATE and MERGE statements.
   *
   * @param targetTable The {@link Table} this update item writes to
   * @param updateItem The ResolvedUpdateItem to return lineage for
   * @param originalStatement The {@link ResolvedStatement} the update item belongs to. Used
   *    *  to resolve the parent columns of the update expression.
   * @return An optional instance of {@link ColumnLineage}, empty if the update item assigns to
   *  something other than a column directly.
   */
  private static Optional<ColumnLineage> extractColumnLevelLineageForUpdateItem(
      Table targetTable,
      ResolvedUpdateItem updateItem,
      ResolvedStatement originalStatement) {

    ResolvedExpr targetExpression = updateItem.getTarget();
    ResolvedExpr updateExpression = updateItem.getSetValue().getValue();

    Optional<ResolvedColumn> maybeTargetColumn = resolveUpdateItemTarget(targetExpression);

    if (!maybeTargetColumn.isPresent()) {
      return Optional.empty();
    }

    ResolvedColumn targetColumn = maybeTargetColumn.get();

    List<ResolvedColumn> parents =
        ParentColumnFinder.findParentsForExpression(originalStatement, updateExpression);

    ColumnLineage result = buildColumnLineage(
        targetTable.getFullName(), targetColumn.getName(), parents);

    return Optional.of(result);
  }

  /**
   * Extracts the column-level lineage entries for a {@link ResolvedUpdateStmt}
   *
   * @param updateStmt The ResolvedUpdateStmt for which to extract lineage
   * @return The set of resulting {@link ColumnLineage} objects
   */
  public static Set<ColumnLineage> extractColumnLevelLineage(ResolvedUpdateStmt updateStmt) {
    Table targetTable = updateStmt.getTableScan().getTable();
    List<ResolvedUpdateItem> updateItems = updateStmt.getUpdateItemList();

    return updateItems.stream()
        .map(updateItem ->
            extractColumnLevelLineageForUpdateItem(targetTable, updateItem, updateStmt))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toSet());
  }

  /**
   * Extracts the column-level lineage entry for a {@link ResolvedMergeWhen}. ResolvedMergeWhens
   * represent a "WHEN [NOT] MATCHED [BY SOURCE|TARGET] THEN ..." clause.
   *
   * @param targetTable The {@link Table} this merge statement item writes to.
   * @param mergeWhen The ResolvedMergeWhen to return lineage for.
   * @param originalStatement The {@link ResolvedMergeStmt} the ResolvedMergeWhen belongs to. Used
   *  to resolve the lineage of the INSERT/UPDATE operations the ResolvedMergeWhen contains.
   * @return The set of resulting {@link ColumnLineage} objects.
   */
  private static Set<ColumnLineage> extractColumnLevelLineage(
      Table targetTable,
      ResolvedMergeWhen mergeWhen,
      ResolvedMergeStmt originalStatement) {

    List<ResolvedColumn> insertedColumns = mergeWhen.getInsertColumnList();
    ResolvedInsertRow insertRow = mergeWhen.getInsertRow();
    List<ResolvedUpdateItem> updateItems = mergeWhen.getUpdateItemList();

    if (Objects.nonNull(insertRow)) {
      // WHEN ... THEN INSERT
      return IntStream.range(0, insertedColumns.size())
          .mapToObj(index -> new SimpleEntry<>(
              insertedColumns.get(index),
              insertRow.getValueList().get(index).getValue()))
          .map(entry -> new SimpleEntry<>(
              entry.getKey(),
              ParentColumnFinder.findParentsForExpression(originalStatement, entry.getValue())))
          .map(entry -> buildColumnLineage(
              targetTable.getFullName(), entry.getKey().getName(), entry.getValue()))
          .collect(Collectors.toSet());
    } else if (Objects.nonNull(updateItems)) {
      // WHEN ... THEN UPDATE
      return updateItems.stream()
          .map(updateItem ->
              extractColumnLevelLineageForUpdateItem(targetTable, updateItem, originalStatement))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .collect(Collectors.toSet());
    }

    return ImmutableSet.of();
  }

  /**
   * Extracts the column-level lineage entries for a {@link ResolvedMergeStmt}
   *
   * @param mergeStmt The ResolvedMergeStmt for which to extract lineage
   * @return The set of resulting {@link ColumnLineage} objects
   */
  public static Set<ColumnLineage> extractColumnLevelLineage(ResolvedMergeStmt mergeStmt) {
    Table targetTable = mergeStmt.getTableScan().getTable();

    return mergeStmt.getWhenClauseList()
        .stream()
        .map(mergeWhen -> extractColumnLevelLineage(targetTable, mergeWhen, mergeStmt))
        .flatMap(Set::stream)
        .collect(Collectors.toSet());
  }

}
