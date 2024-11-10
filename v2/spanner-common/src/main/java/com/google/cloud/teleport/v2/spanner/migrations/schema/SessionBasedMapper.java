/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.schema;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.curator.shaded.com.google.common.collect.ImmutableList;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This mapper uses an SMT session file to map table and column names. For fetching destination data
 * types, it uses {@link Ddl}.
 */
public class SessionBasedMapper implements ISchemaMapper, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(SessionBasedMapper.class);

  private final Ddl ddl;

  private final Schema schema;

  /*If enabled, throw error on mismatches between spanner schema and session file. Defaults to false.
   */
  private boolean strictCheckSchema = false;

  public SessionBasedMapper(String sessionFilePath, Ddl ddl) throws InputMismatchException {
    this(sessionFilePath, ddl, false);
  }

  public SessionBasedMapper(String sessionFilePath, Ddl ddl, boolean strictCheckSchema)
      throws InputMismatchException {
    this(SessionFileReader.read(sessionFilePath), ddl, strictCheckSchema);
  }

  public SessionBasedMapper(Schema schema, Ddl ddl) throws InputMismatchException {
    this(schema, ddl, false);
  }

  public SessionBasedMapper(Schema schema, Ddl ddl, boolean strictCheckSchema)
      throws InputMismatchException {
    this.schema = schema;
    this.ddl = ddl;
    try {
      validateSchemaAndDdl(schema, ddl);
      LOG.info("schema matches between session file and spanner");
    } catch (InputMismatchException e) {
      if (strictCheckSchema) {
        LOG.warn("schema does not match between session and spanner: {}", e.getMessage());
        throw e;
      } else {
        LOG.warn("proceeding without schema match between session and spanner");
      }
    }
  }

  static void validateSchemaAndDdl(Schema schema, Ddl ddl) throws InputMismatchException {
    List<String> schemaTableNames = new ArrayList<>(schema.getToSource().keySet());
    Collections.sort(schemaTableNames);
    List<String> ddlTableNames =
        ddl.allTables().stream().map(table -> table.name()).collect(Collectors.toList());
    Collections.sort(ddlTableNames);
    if (!schemaTableNames.equals(ddlTableNames)) {
      throw new InputMismatchException(
          String.format(
              "List of spanner table names found in session file do not match tables that actually "
                  + "exist on Spanner. Please provide a valid session file. spanner tables: %s session "
                  + "tables: %s",
              ddlTableNames, schemaTableNames));
    }
    for (String tableName : ddlTableNames) {
      List<String> schemaColNames = schema.getSpannerColumnNames(tableName);
      Collections.sort(schemaColNames);
      List<String> ddlColNames =
          ddl.table(tableName).columns().stream()
              .map(column -> column.name())
              .collect(Collectors.toList());
      Collections.sort(ddlColNames);
      if (!schemaColNames.equals(ddlColNames)) {
        throw new InputMismatchException(
            String.format(
                "List of spanner column names found in session file do not match columns that "
                    + "actually exist on Spanner for table '%s'. Please provide a valid session "
                    + "file. SessionColumnNames: '%s' SpannerColumnNames: '%s'",
                tableName, schemaColNames, ddlColNames));
      }
    }
  }

  @Override
  public Dialect getDialect() {
    return ddl.dialect();
  }

  public List<String> getSourceTablesToMigrate(String namespace) {
    if (!Strings.isNullOrEmpty(namespace)) {
      throw new UnsupportedOperationException(
          "can not resolve namespace and namespace support " + "is not added yet: " + namespace);
    }
    return ImmutableList.copyOf(schema.getToSpanner().keySet());
  }

  @Override
  public String getSourceTableName(String namespace, String spTable) throws NoSuchElementException {
    Map<String, NameAndCols> toSource = schema.getToSource();
    if (!toSource.containsKey(spTable)) {
      throw new NoSuchElementException(
          String.format("Spanner table '%s' equivalent not found in source", spTable));
    }
    return toSource.get(spTable).getName();
  }

  @Override
  public String getSpannerTableName(String namespace, String srcTable)
      throws NoSuchElementException {
    Map<String, NameAndCols> toSpanner = schema.getToSpanner();
    if (!toSpanner.containsKey(srcTable)) {
      throw new NoSuchElementException(String.format("Source table '%s' not found", srcTable));
    }
    return toSpanner.get(srcTable).getName();
  }

  @Override
  public String getSpannerColumnName(String namespace, String srcTable, String srcColumn)
      throws NoSuchElementException {
    Map<String, NameAndCols> toSpanner = schema.getToSpanner();
    if (!toSpanner.containsKey(srcTable)) {
      throw new NoSuchElementException(String.format("Source table '%s' not found", srcTable));
    }
    Map<String, String> cols = toSpanner.get(srcTable).getCols();
    if (!cols.containsKey(srcColumn)) {
      throw new NoSuchElementException(
          String.format("Source column '%s' not found for table '%s'", srcColumn, srcTable));
    }
    return cols.get(srcColumn);
  }

  @Override
  public String getSourceColumnName(String namespace, String spannerTable, String spannerColumn)
      throws NoSuchElementException {
    Map<String, NameAndCols> toSource = schema.getToSource();
    if (!toSource.containsKey(spannerTable)) {
      throw new NoSuchElementException(String.format("Spanner table '%s' not found", spannerTable));
    }
    Map<String, String> cols = toSource.get(spannerTable).getCols();
    if (!cols.containsKey(spannerColumn)) {
      throw new NoSuchElementException(
          String.format(
              "Spanner column '%s' not found for table '%s'", spannerColumn, spannerTable));
    }
    return cols.get(spannerColumn);
  }

  @Override
  public Type getSpannerColumnType(String namespace, String spannerTable, String spannerColumn)
      throws NoSuchElementException {
    Table spTable = ddl.table(spannerTable);
    if (spTable == null) {
      throw new NoSuchElementException(String.format("Spanner table '%s' not found", spannerTable));
    }
    Column col = spTable.column(spannerColumn);
    if (col == null) {
      throw new NoSuchElementException(
          String.format("Spanner column '%s' not found", spannerColumn));
    }
    return col.type();
  }

  @Override
  public List<String> getSpannerColumns(String namespace, String spannerTable)
      throws NoSuchElementException {
    return schema.getSpannerColumnNames(spannerTable);
  }

  @Override
  public String getShardIdColumnName(String namespace, String spannerTableName) {
    Map<String, NameAndCols> spanToId = schema.getSpannerToID();
    if (!spanToId.containsKey(spannerTableName)) {
      throw new NoSuchElementException(
          String.format("Spanner table '%s' not found", spannerTableName));
    }

    String tableId =
        Objects.requireNonNull(
                spanToId.get(spannerTableName),
                String.format(
                    "Found null table in spanToId for table %s, please provide a valid session file.",
                    spannerTableName))
            .getName();
    Objects.requireNonNull(
        tableId,
        String.format(
            "Found null table id for table %s, please provide a valid session file.",
            spannerTableName));
    SpannerTable table =
        Objects.requireNonNull(
                schema.getSpSchema(), "Found null spSchema, please provide a valid session file.")
            .get(tableId);
    Objects.requireNonNull(
        table,
        String.format(
            "Found null table for tableId %s, please provide a valid session file.", tableId));

    String colId = table.getShardIdColumn();
    if (Strings.isNullOrEmpty(colId)) {
      return null;
    }
    Objects.requireNonNull(
        table.getColDefs(),
        String.format(
            "Found null col defs for table %s, please provide a valid session file.",
            spannerTableName));
    SpannerColumnDefinition shardColDef =
        Objects.requireNonNull(
            table.getColDefs().get(colId),
            String.format(
                "No col def found for colId %s, please provide a valid session file.", colId));
    return Objects.requireNonNull(
        shardColDef.getName(),
        String.format(
            "Found null shard col name for table %s, colId %s, please provide a valid session file.",
            spannerTableName, colId));
  }

  @Override
  public String getSyntheticPrimaryKeyColName(String namespace, String spannerTableName) {
    // Get the table ID mapping or throw if table not found
    NameAndCols tableMapping =
        Optional.ofNullable(schema.getSpannerToID().get(spannerTableName))
            .orElseThrow(
                () ->
                    new NoSuchElementException(
                        String.format("Spanner table '%s' not found", spannerTableName)));

    String tableId =
        Optional.ofNullable(tableMapping.getName())
            .orElseThrow(
                () ->
                    new NoSuchElementException(
                        String.format("Invalid table ID for table %s", spannerTableName)));

    // If no synthetic PK exists for this table, return null
    SyntheticPKey synthPk = schema.getSyntheticPks().get(tableId);
    if (synthPk == null) {
      return null;
    }

    // Get the column definition and return its name
    SpannerTable table =
        Optional.ofNullable(schema.getSpSchema().get(tableId))
            .orElseThrow(
                () ->
                    new NoSuchElementException(
                        String.format("Table %s not found in schema", tableId)));

    return Optional.ofNullable(table.getColDefs())
        .map(cols -> cols.get(synthPk.getColId()))
        .map(SpannerColumnDefinition::getName)
        .orElseThrow(
            () ->
                new NoSuchElementException(
                    String.format("Invalid column definition for table %s", spannerTableName)));
  }

  @Override
  public boolean colExistsAtSource(String namespace, String spannerTable, String spannerColumn) {
    try {
      getSourceColumnName(namespace, spannerTable, spannerColumn);
      return true;
    } catch (NoSuchElementException e) {
      return false;
    }
  }
}
