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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.List;
import java.util.Map;

public class SchemaUtils {

  public static Ddl buildDdlFromSessionFile(String sessionFile) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> session = mapper.readValue(new File(sessionFile), Map.class);
      Map<String, Object> spSchema = (Map<String, Object>) session.get("SpSchema");
      Ddl.Builder ddlBuilder = Ddl.builder();
      for (Map.Entry<String, Object> entry : spSchema.entrySet()) {
        Map<String, Object> tableMap = (Map<String, Object>) entry.getValue();
        String tableName = (String) tableMap.get("Name");
        Table.Builder tableBuilder = ddlBuilder.createTable(tableName);
        Map<String, Object> colDefs = (Map<String, Object>) tableMap.get("ColDefs");
        for (String colId : ((Map<String, Object>) colDefs).keySet()) {
          Map<String, Object> colMap = (Map<String, Object>) colDefs.get(colId);
          String colName = (String) colMap.get("Name");
          Map<String, Object> typeMap = (Map<String, Object>) colMap.get("T");
          String typeName = (String) typeMap.get("Name");
          Boolean isArray = (Boolean) typeMap.get("IsArray");
          if (typeName.equals("STRING")) {
            if (isArray) {
              tableBuilder.column(colName).array(Type.string()).endColumn();
            } else {
              tableBuilder.column(colName).string().max().endColumn();
            }
          } else if (typeName.equals("INT64")) {
            tableBuilder.column(colName).int64().endColumn();
          } else if (typeName.equals("FLOAT32")) {
            tableBuilder.column(colName).float32().endColumn();
          } else if (typeName.equals("FLOAT64")) {
            tableBuilder.column(colName).float64().endColumn();
          } else if (typeName.equals("BOOL")) {
            tableBuilder.column(colName).bool().endColumn();
          } else if (typeName.equals("BYTES")) {
            tableBuilder.column(colName).bytes().max().endColumn();
          } else if (typeName.equals("TIMESTAMP")) {
            tableBuilder.column(colName).timestamp().endColumn();
          } else if (typeName.equals("DATE")) {
            tableBuilder.column(colName).date().endColumn();
          } else if (typeName.equals("NUMERIC")) {
            tableBuilder.column(colName).numeric().endColumn();
          } else if (typeName.equals("JSON")) {
            tableBuilder.column(colName).json().endColumn();
          } else {
            throw new IllegalArgumentException(
                "This spanner type in session file is not yet supported");
          }
          // TODO: Add other types like arrays etc.
        }
        List<Map<String, Object>> pks = (List<Map<String, Object>>) tableMap.get("PrimaryKeys");
        if (pks != null && !pks.isEmpty()) {
          IndexColumn.IndexColumnsBuilder<Table.Builder> pkBuilder = tableBuilder.primaryKey();
          for (Map<String, Object> pk : pks) {
            String colId = (String) pk.get("ColId");
            Map<String, Object> colMap = (Map<String, Object>) colDefs.get(colId);
            String colName = (String) colMap.get("Name");
            pkBuilder.asc(colName);
          }
          pkBuilder.end();
        }
        tableBuilder.endTable();
      }
      return ddlBuilder.build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static SourceSchema buildSourceSchemaFromSessionFile(String sessionFile) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> session = mapper.readValue(new File(sessionFile), Map.class);

      SourceDatabaseType dbType = SourceDatabaseType.MYSQL;
      String dbName = "test-db";
      Map<String, Object> srcSchema = (Map<String, Object>) session.get("SrcSchema");
      SourceSchema.Builder schemaBuilder = SourceSchema.builder(dbType).databaseName(dbName);
      ImmutableMap.Builder<String, SourceTable> tablesBuilder = ImmutableMap.builder();
      if (srcSchema != null) {
        for (Map.Entry<String, Object> entry : srcSchema.entrySet()) {
          Map<String, Object> tableMap = (Map<String, Object>) entry.getValue();
          SourceTable.Builder tableBuilder =
              SourceTable.builder(dbType)
                  .name((String) tableMap.get("Name"))
                  .schema((String) tableMap.get("Schema"));
          Map<String, Object> colDefs = (Map<String, Object>) tableMap.get("ColDefs");
          ImmutableList.Builder<SourceColumn> columnsBuilder = ImmutableList.builder();
          if (colDefs != null) {
            for (String colId : ((Map<String, Object>) colDefs).keySet()) {
              Map<String, Object> colMap = (Map<String, Object>) colDefs.get(colId);
              Map<String, Object> typeMap = (Map<String, Object>) colMap.get("Type");
              String typeName = (String) typeMap.get("Name");
              Integer size = null;
              if (typeMap.get("Len") instanceof Number) {
                size = ((Number) typeMap.get("Len")).intValue();
              }
              SourceColumn.Builder colBuilder =
                  SourceColumn.builder(dbType)
                      .name((String) colMap.get("Name"))
                      .type(typeName)
                      .isNullable(
                          !(colMap.get("NotNull") != null && (Boolean) colMap.get("NotNull")))
                      .size(size);
              columnsBuilder.add(colBuilder.build());
            }
          }
          tableBuilder.columns(columnsBuilder.build());
          ImmutableList.Builder<String> pkCols = ImmutableList.builder();
          if (tableMap.get("PrimaryKeys") != null) {
            for (Map<String, Object> pk :
                (Iterable<Map<String, Object>>) tableMap.get("PrimaryKeys")) {
              String colId = (String) pk.get("ColId");
              String colName = ((Map<String, Object>) colDefs.get(colId)).get("Name").toString();
              pkCols.add(colName);
            }
          }
          tableBuilder.primaryKeyColumns(pkCols.build());
          tablesBuilder.put((String) tableMap.get("Name"), tableBuilder.build());
        }
      }
      schemaBuilder.tables(tablesBuilder.build());
      return schemaBuilder.build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
