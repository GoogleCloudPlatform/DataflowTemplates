/*
 * Copyright (C) 2025 Google LLC
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
import com.google.cloud.teleport.v2.spanner.ddl.annotations.cassandra.CassandraAnnotations;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schema mapper that uses a file based schema overrides parser. Assumes that if a table or column
 * is not explicitly overridden, its name is the same in the source and Spanner.
 */
public class SchemaFileOverridesBasedMapper implements ISchemaMapper, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaFileOverridesBasedMapper.class);
  private final SchemaFileOverridesParser parser;
  private final Ddl ddl;

  /**
   * Constructs a new SchemaFileOverridesBasedMapper.
   *
   * @param schemaOverridesFilePath Path to the schema overrides JSON file.
   * @param ddl The DDL representation of the Spanner schema.
   */
  public SchemaFileOverridesBasedMapper(String schemaOverridesFilePath, Ddl ddl) {
    this.parser = new SchemaFileOverridesParser(schemaOverridesFilePath);
    this.ddl = ddl;
  }

  @Override
  public Dialect getDialect() {
    return ddl.dialect();
  }

  @Override
  public List<String> getSourceTablesToMigrate(String namespace) {
    // For each table in the Spanner DDL, find its corresponding source table name.
    // Namespace is ignored.
    return ddl.allTables().stream()
        .map(spannerTableInDdl -> getSourceTableName(namespace, spannerTableInDdl.name()))
        .collect(Collectors.toList());
  }

  @Override
  public String getSourceTableName(String namespace, String spTable) throws NoSuchElementException {
    // Ensure Spanner table exists in DDL first.
    // Namespace is ignored.
    if (ddl.table(spTable) == null) {
      throw new NoSuchElementException("Spanner table '" + spTable + "' not found in DDL.");
    }

    // Check if spTable is a target of a rename in the overrides.
    if (parser.schemaFileOverride != null && parser.schemaFileOverride.getRenamedTables() != null) {
      for (Map.Entry<String, String> entry :
          parser.schemaFileOverride.getRenamedTables().entrySet()) {
        // If the value (Spanner name in override) matches spTable, the key is the source name.
        if (entry.getValue().equals(spTable)) {
          return entry.getKey();
        }
      }
    }
    // If not found in overrides as a target, assume source name is the same as the Spanner name.
    return spTable;
  }

  @Override
  public String getSpannerTableName(String namespace, String srcTable)
      throws NoSuchElementException {
    String spannerTableNameCandidate = parser.getTableOverride(srcTable);

    // Validate that the resolved Spanner table exists in the DDL.
    if (ddl.table(spannerTableNameCandidate) == null) {
      throw new NoSuchElementException(
          String.format(
              "Resolved Spanner table '%s' (from source table '%s') not found in DDL.",
              spannerTableNameCandidate, srcTable));
    }
    return spannerTableNameCandidate;
  }

  @Override
  public String getSpannerColumnName(String namespace, String srcTable, String srcColumn)
      throws NoSuchElementException {

    String spannerTableName = getSpannerTableName(namespace, srcTable);

    // Get the candidate Spanner column name from overrides for the original srcTable and srcColumn.
    String spannerColumnNameCandidate = parser.getColumnOverride(srcTable, srcColumn);

    // Validate that the resolved Spanner column exists in the Spanner table in the DDL.
    Table spTableInDdl = ddl.table(spannerTableName);
    Column spColInDdl = spTableInDdl.column(spannerColumnNameCandidate);
    if (spColInDdl == null) {
      throw new NoSuchElementException(
          String.format(
              "Resolved Spanner column '%s' (from source column '%s.%s') not found in Spanner table '%s' in DDL.",
              spannerColumnNameCandidate, srcTable, srcColumn, spannerTableName));
    }
    return spannerColumnNameCandidate;
  }

  @Override
  public String getSourceColumnName(String namespace, String spannerTable, String spannerColumn)
      throws NoSuchElementException {
    // Ensure Spanner table and column exist in DDL first.
    // Namespace is ignored.
    Table table = ddl.table(spannerTable);
    if (table == null) {
      throw new NoSuchElementException("Spanner table '" + spannerTable + "' not found in DDL.");
    }
    if (table.column(spannerColumn) == null) {
      throw new NoSuchElementException(
          "Spanner column '"
              + spannerColumn
              + "' not found in Spanner table '"
              + spannerTable
              + "' in DDL.");
    }

    // Determine the original source table name for the given spannerTable.
    String sourceTableNameForLookup = getSourceTableName(namespace, spannerTable);

    // Check if spannerColumn is a target of a column rename within sourceTableNameForLookup.
    if (parser.schemaFileOverride != null
        && parser.schemaFileOverride.getRenamedColumnTupleMap() != null) {
      Map<String, String> columnOverrides =
          parser.schemaFileOverride.getRenamedColumnTupleMap().get(sourceTableNameForLookup);
      if (columnOverrides != null) {
        for (Map.Entry<String, String> entry : columnOverrides.entrySet()) {
          // If the value (Spanner column name in override) matches spannerColumn, the key is the
          // source column name.
          if (entry.getValue().equals(spannerColumn)) {
            return entry.getKey();
          }
        }
      }
    }
    // If not found in overrides, assume source column name is the same as the Spanner column name.
    return spannerColumn;
  }

  @Override
  public Type getSpannerColumnType(String namespace, String spannerTable, String spannerColumn)
      throws NoSuchElementException {
    // Namespace is ignored.
    Table table = ddl.table(spannerTable);
    if (table == null) {
      throw new NoSuchElementException("Spanner table '" + spannerTable + "' not found in DDL.");
    }
    Column column = table.column(spannerColumn);
    if (column == null) {
      throw new NoSuchElementException(
          "Spanner column '"
              + spannerColumn
              + "' not found in Spanner table '"
              + spannerTable
              + "' in DDL.");
    }
    return column.type();
  }

  @Override
  public CassandraAnnotations getSpannerColumnCassandraAnnotations(
      String namespace, String spannerTable, String spannerColumn) throws NoSuchElementException {
    // Namespace is ignored.
    Table table = ddl.table(spannerTable);
    if (table == null) {
      throw new NoSuchElementException("Spanner table '" + spannerTable + "' not found in DDL.");
    }
    Column column = table.column(spannerColumn);
    if (column == null) {
      throw new NoSuchElementException(
          "Spanner column '"
              + spannerColumn
              + "' not found in Spanner table '"
              + spannerTable
              + "' in DDL.");
    }
    return column.cassandraAnnotation();
  }

  @Override
  public List<String> getSpannerColumns(String namespace, String spannerTable)
      throws NoSuchElementException {
    // Namespace is ignored.
    Table table = ddl.table(spannerTable);
    if (table == null) {
      throw new NoSuchElementException("Spanner table '" + spannerTable + "' not found in DDL.");
    }
    return table.columns().stream().map(Column::name).collect(Collectors.toList());
  }

  @Override
  public String getShardIdColumnName(String namespace, String spannerTableName) {
    LOG.warn("For schema override based migrations, the shard id must be supplied via custom jar.");
    return null;
  }

  @Override
  public String getSyntheticPrimaryKeyColName(String namespace, String spannerTableName) {
    LOG.warn("Synthetic PK are not supported for schema override based migrations.");
    return null;
  }

  @Override
  public boolean colExistsAtSource(String namespace, String spannerTable, String spannerColumn) {
    // Namespace is ignored.
    try {
      // If getSourceColumnName succeeds, it implies the Spanner table/column exist in DDL
      // and a mapping to a source column (even if default/same name) was found.
      getSourceColumnName(namespace, spannerTable, spannerColumn);
      return true;
    } catch (NoSuchElementException e) {
      // Thrown if Spanner table/column not in DDL, or mapping fails.
      return false;
    }
  }
}
