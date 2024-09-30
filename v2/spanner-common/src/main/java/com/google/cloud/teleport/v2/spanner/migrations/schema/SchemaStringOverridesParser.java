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

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/** Encodes the user specified schema overrides for retrieval during source to spanner mapping. */
public class SchemaStringOverridesParser implements ISchemaOverridesParser, Serializable {

  @VisibleForTesting final Map<String, String> tableNameOverrides;
  @VisibleForTesting final Map<String, Pair<String, String>> columnNameOverrides;

  public SchemaStringOverridesParser(Map<String, String> userOptionOverrides) {
    tableNameOverrides = new HashMap<>();
    columnNameOverrides = new HashMap<>();
    if (userOptionOverrides.containsKey("tableOverrides")) {
      parseTableMapping(userOptionOverrides.get("tableOverrides"));
    }
    if (userOptionOverrides.containsKey("columnOverrides")) {
      parseColumnMapping(userOptionOverrides.get("columnOverrides"));
    }
    validateMapping();
  }

  /**
   * Gets the spanner table name given the source table name, or source table name if no override is
   * configured.
   *
   * @param sourceTableName The source table name
   * @return The overridden spanner table name
   */
  @Override
  public String getTableOverride(String sourceTableName) {
    return tableNameOverrides.getOrDefault(sourceTableName, sourceTableName);
  }

  /**
   * Gets the spanner column name given the source table name, or the source column name if override
   * is configured.
   *
   * @param sourceTableName the source table name for which column name is overridden
   * @param sourceColumnName the source column name being overridden
   * @return A pair of spannerTableName and spannerColumnName
   */
  @Override
  public Pair<String, String> getColumnOverride(String sourceTableName, String sourceColumnName) {
    return columnNameOverrides.getOrDefault(
        String.format("%s.%s", sourceTableName, sourceColumnName),
        new ImmutablePair<>(sourceTableName, sourceColumnName));
  }

  private void parseTableMapping(String tableInput) {
    String pairsString = tableInput.substring(1, tableInput.length() - 1);
    int openBraceCount = 0;
    int startIndex = 0;
    for (int i = 0; i < pairsString.length(); i++) {
      char c = pairsString.charAt(i);
      if (c == '{') {
        openBraceCount++;
        if (openBraceCount == 1) {
          startIndex = i;
        }
      } else if (c == '}') {
        openBraceCount--;
        if (openBraceCount == 0) {
          String pair = pairsString.substring(startIndex + 1, i);
          String[] tables = pair.split(",");
          if (tables.length == 2) {
            String sourceTable = tables[0].trim();
            String destinationTable = tables[1].trim();
            tableNameOverrides.put(sourceTable, destinationTable);
          } else {
            throw new IllegalArgumentException(
                String.format(
                    "Malformed pair encountered: %s, please fix the tableOverrides and rerun the job.",
                    pair));
          }
        }
      }
    }
  }

  private void parseColumnMapping(String columnInput) {
    String pairsString = columnInput.substring(1, columnInput.length() - 1);
    int openBraceCount = 0;
    int startIndex = 0;
    for (int i = 0; i < pairsString.length(); i++) {
      char c = pairsString.charAt(i);
      if (c == '{') {
        openBraceCount++;
        if (openBraceCount == 1) {
          startIndex = i;
        }
      } else if (c == '}') {
        openBraceCount--;
        if (openBraceCount == 0) {
          String pair = pairsString.substring(startIndex + 1, i);
          String[] tableColumnPairs = pair.split(",");
          if (tableColumnPairs.length == 2) {
            String[] sourceTableColumn = tableColumnPairs[0].trim().split("\\.");
            String[] destTableColumn = tableColumnPairs[1].trim().split("\\.");
            if (sourceTableColumn.length == 2 && destTableColumn.length == 2) {
              columnNameOverrides.put(
                  String.format("%s.%s", sourceTableColumn[0], sourceTableColumn[1]),
                  new ImmutablePair<>(destTableColumn[0], destTableColumn[1]));
            } else {
              throw new IllegalArgumentException(
                  String.format(
                      "Malformed pair encountered: %s, please fix the columnOverrides and rerun the job.",
                      pair));
            }
          } else {
            throw new IllegalArgumentException(
                String.format(
                    "Malformed pair encountered: %s, please fix the columnOverrides and rerun the job.",
                    pair));
          }
        }
      }
    }
  }

  private void validateMapping() {
    columnNameOverrides
        .keySet()
        .forEach(
            sourceKey -> {
              String[] sourceTableColumn = sourceKey.split("\\.");
              String spTableFromColumn = columnNameOverrides.get(sourceKey).getLeft();
              // in columns overrides, table names should not change between source and spanner.
              if (!sourceTableColumn[0].equals(spTableFromColumn)) {
                throw new IllegalArgumentException(
                    String.format(
                        "tableNames have been changed between source and spanner pairs, columnOverrides should"
                            + "only be used to specify column name overrides, and the same source table name"
                            + "should be used to form the columnOverride pair. Please use tableOverrides to"
                            + "specify table name overrides."
                            + "The sourceTable is %s while the spannerTable is %s",
                        sourceTableColumn[0], spTableFromColumn));
              }
            });
  }

  @Override
  public String toString() {
    return "SchemaOverridesParser{"
        + "tableNameOverrides="
        + tableNameOverrides
        + ", columnNameOverrides="
        + columnNameOverrides
        + '}';
  }
}
