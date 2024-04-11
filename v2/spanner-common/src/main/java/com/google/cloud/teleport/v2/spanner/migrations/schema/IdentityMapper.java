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
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * This mapper directly maps table and column names without any modification. For fetching
 * destination data types, it uses {@link Ddl}.
 */
public class IdentityMapper implements ISchemaMapper {

  private final Ddl ddl;

  public IdentityMapper(Ddl ddl) {
    this.ddl = ddl;
  }

  @Override
  public String getSpannerTableName(String namespace, String srcTable)
      throws NoSuchElementException {
    if (ddl.table(srcTable) == null) {
      throw new NoSuchElementException(String.format("Spanner table '%s' not found", srcTable));
    }
    return srcTable;
  }

  @Override
  public String getSpannerColumnName(String namespace, String srcTable, String srcColumn)
      throws NoSuchElementException {
    Table spTable = ddl.table(srcTable);
    if (spTable == null) {
      throw new NoSuchElementException(String.format("Spanner table '%s' not found", srcTable));
    }
    if (spTable.column(srcColumn) == null) {
      throw new NoSuchElementException(
          String.format("Spanner column '%s' not found for table '%s'", srcColumn, srcTable));
    }
    return srcColumn;
  }

  @Override
  public String getSourceColumnName(String namespace, String spannerTable, String spannerColumn) {
    return spannerColumn;
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
    Table spTable = ddl.table(spannerTable);
    if (spTable == null) {
      throw new NoSuchElementException(String.format("Spanner table '%s' not found", spannerTable));
    }
    return spTable.columns().stream().map(column -> column.name()).collect(Collectors.toList());
  }
}
