/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.dofn;

import com.github.javafaker.Faker;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorUniqueKey;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.cloud.teleport.v2.templates.utils.DataGeneratorUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/**
 * Pure-function helpers for assembling Beam {@link Row}s from a {@link DataGeneratorTable}, plus
 * primary-key utilities used to drive state-keying inside {@code BatchAndWriteFn}.
 *
 * <p>This class has no Beam runtime state and no I/O. It exists to keep {@code BatchAndWriteFn}
 * focused on lifecycle / state / batching concerns while the row-construction details live here.
 */
final class RowAssembler {

  private static final Map<DataGeneratorTable, Set<String>> UNIQUE_COLUMNS_CACHE =
      new ConcurrentHashMap<>();
  private static final Map<DataGeneratorTable, Set<String>> FK_COLUMNS_CACHE =
      new ConcurrentHashMap<>();

  private RowAssembler() {}

  /** Returns the names of all columns covered by any unique key on {@code table}. */
  static Set<String> uniqueColumnNames(DataGeneratorTable table) {
    Set<String> uniqueColumns = new HashSet<>();
    if (table.uniqueKeys() != null) {
      for (DataGeneratorUniqueKey uk : table.uniqueKeys()) {
        uniqueColumns.addAll(uk.columns());
      }
    }
    return uniqueColumns;
  }

  /** Returns the names of every column referenced by any foreign key on {@code table}. */
  static Set<String> foreignKeyColumns(DataGeneratorTable table) {
    Set<String> fkColumns = new HashSet<>();
    if (table.foreignKeys() != null) {
      for (DataGeneratorForeignKey fk : table.foreignKeys()) {
        fkColumns.addAll(fk.keyColumns());
      }
    }
    return fkColumns;
  }

  /**
   * Builds a row for an UPDATE event. Primary keys come from {@code pkValues}; foreign keys and
   * unique columns are preserved from {@code originalRow} so they don't churn between updates;
   * everything else is freshly generated.
   *
   * <p>{@code col.isSkipped()} columns are omitted from the row schema entirely so the sink writes
   * its DEFAULT (or NULL) for them.
   */
  static Row generateUpdateRow(
      LinkedHashMap<String, Object> pkValues,
      DataGeneratorTable table,
      Row originalRow,
      Faker faker) {
    Schema.Builder schemaBuilder = Schema.builder();
    List<Object> values = new ArrayList<>();
    Set<String> pkSet = new HashSet<>(table.primaryKeys());
    Set<String> fkColumns = FK_COLUMNS_CACHE.computeIfAbsent(table, k -> foreignKeyColumns(table));
    Set<String> uniqueColumns =
        UNIQUE_COLUMNS_CACHE.computeIfAbsent(table, k -> uniqueColumnNames(table));

    for (DataGeneratorColumn col : table.columns()) {
      if (col.isSkipped()) {
        continue;
      }
      schemaBuilder.addField(
          Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
      if (pkSet.contains(col.name())) {
        values.add(pkValues.get(col.name()));
      } else if (uniqueColumns.contains(col.name())) {
        // Unique columns must NOT churn on UPDATE. Preserve the original inserted value from
        // state — re-deriving would either collide with another row's existing value or change
        // the row's logical identity between updates. createReducedRow guarantees every unique
        // column is captured at INSERT time.
        Object originalVal =
            (originalRow != null && originalRow.getSchema().hasField(col.name()))
                ? originalRow.getValue(col.name())
                : null;
        values.add(originalVal);
      } else if (fkColumns.contains(col.name())) {
        Object val =
            (originalRow != null && originalRow.getSchema().hasField(col.name()))
                ? originalRow.getValue(col.name())
                : DataGeneratorUtils.generateValue(col, faker);
        values.add(val);
      } else {
        values.add(DataGeneratorUtils.generateValue(col, faker));
      }
    }
    return Row.withSchema(schemaBuilder.build()).addValues(values).build();
  }

  /**
   * Builds a row for a DELETE event. Only the primary-key columns carry values; everything else is
   * a nullable {@code null}. {@code isSkipped()} columns are omitted entirely.
   */
  static Row generateDeleteRow(LinkedHashMap<String, Object> pkValues, DataGeneratorTable table) {
    Schema.Builder schemaBuilder = Schema.builder();
    List<Object> values = new ArrayList<>();
    Set<String> pkSet = new HashSet<>(table.primaryKeys());

    for (DataGeneratorColumn col : table.columns()) {
      if (col.isSkipped()) {
        continue;
      }
      Schema.FieldType fieldType = DataGeneratorUtils.mapToBeamFieldType(col.logicalType());
      if (pkSet.contains(col.name())) {
        schemaBuilder.addField(Schema.Field.of(col.name(), fieldType));
        values.add(pkValues.get(col.name()));
      } else {
        schemaBuilder.addField(Schema.Field.nullable(col.name(), fieldType));
        values.add(null);
      }
    }
    return Row.withSchema(schemaBuilder.build()).addValues(values).build();
  }

  /**
   * Captures only the columns a future UPDATE/DELETE follow-up will need: PK + FK + unique columns,
   * plus the synthetic shard id when present. {@code isSkipped()} columns are omitted.
   */
  static Row createReducedRow(Row fullRow, DataGeneratorTable table) {
    Schema.Builder schemaBuilder = Schema.builder();
    List<Object> values = new ArrayList<>();
    Set<String> pkSet = new HashSet<>(table.primaryKeys());
    Set<String> fkColumns = FK_COLUMNS_CACHE.computeIfAbsent(table, k -> foreignKeyColumns(table));
    Set<String> uniqueColumns =
        UNIQUE_COLUMNS_CACHE.computeIfAbsent(table, k -> uniqueColumnNames(table));

    for (DataGeneratorColumn col : table.columns()) {
      if (col.isSkipped()) {
        continue;
      }
      if (pkSet.contains(col.name())
          || fkColumns.contains(col.name())
          || uniqueColumns.contains(col.name())) {
        schemaBuilder.addField(
            Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
        values.add(fullRow.getValue(col.name()));
      }
    }
    if (fullRow.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)) {
      schemaBuilder.addField(
          Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING));
      values.add(fullRow.getString(Constants.SHARD_ID_COLUMN_NAME));
    }
    return Row.withSchema(schemaBuilder.build()).addValues(values).build();
  }

  /**
   * Completes a partial row by generating any missing columns through {@link
   * DataGeneratorUtils#generateValue}. {@code isSkipped()} columns are omitted from the assembled
   * row so the sink writes its DEFAULT.
   *
   * <p>If {@code partialRow} already contains every non-skipped column, it is returned unchanged.
   */
  static Row completeRow(DataGeneratorTable table, Row partialRow, Faker faker) {
    boolean hasAllColumns = true;
    for (DataGeneratorColumn col : table.columns()) {
      if (col.isSkipped()) {
        continue;
      }
      if (!partialRow.getSchema().hasField(col.name())) {
        hasAllColumns = false;
        break;
      }
    }
    if (hasAllColumns) {
      return partialRow;
    }

    Schema.Builder schemaBuilder = Schema.builder();
    List<Object> values = new ArrayList<>();
    for (DataGeneratorColumn col : table.columns()) {
      if (col.isSkipped()) {
        continue;
      }
      Object val =
          partialRow.getSchema().hasField(col.name())
              ? partialRow.getValue(col.name())
              : DataGeneratorUtils.generateValue(col, faker);
      schemaBuilder.addField(
          Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
      values.add(val);
    }

    if (partialRow.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)) {
      String shardId = partialRow.getString(Constants.SHARD_ID_COLUMN_NAME);
      schemaBuilder.addField(
          Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING));
      values.add(shardId);
    }

    return Row.withSchema(schemaBuilder.build()).addValues(values).build();
  }
}
