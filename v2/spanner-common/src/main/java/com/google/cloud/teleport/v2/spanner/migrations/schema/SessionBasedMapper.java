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

import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * This mapper uses an SMT session file to map table and column names. For fetching destination data
 * types, it uses {@link Ddl}.
 */
public class SessionBasedMapper implements ISchemaMapper {

  private final Ddl ddl;

  private final Schema schema;

  public SessionBasedMapper(String sessionFilePath, Ddl ddl) throws InputMismatchException {
    this.schema = SessionFileReader.read(sessionFilePath);
    this.ddl = ddl;
    validateSchemaAndDdl(schema, ddl);
  }

  static void validateSchemaAndDdl(Schema schema, Ddl ddl) throws InputMismatchException {
    List<String> schemaTableNames = new ArrayList<>(schema.getToSource().keySet());
    Collections.sort(schemaTableNames);
    List<String> ddlTableNames =
        ddl.allTables().stream().map(table -> table.name()).collect(Collectors.toList());
    Collections.sort(ddlTableNames);
    if (!schemaTableNames.equals(ddlTableNames)) {
      throw new InputMismatchException(
          "List of spanner table names found in session file do not match tables that actually exist on Spanner. Please provide a valid session file.");
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
                "List of spanner column names found in session file do not match columns that actually exist on Spanner for table '%s'. Please provide a valid session file.",
                tableName));
      }
    }
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
}
