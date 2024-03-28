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
package com.google.cloud.teleport.v2.spanner.migrations.convertors;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class for util methods processing change events and Spanner structures (ex - {@link
 * com.google.cloud.spanner.Mutation}, {@link com.google.cloud.spanner.Key}).
 */
public class ChangeEventSpannerConvertor {
  public static Mutation mutationFromEvent(
      Table table, JsonNode changeEvent, List<String> columnNames, Set<String> keyColumnNames)
      throws ChangeEventConvertorException {
    return mutationBuilderFromEvent(table, changeEvent, columnNames, keyColumnNames).build();
  }

  public static Mutation.WriteBuilder mutationBuilderFromEvent(
      Table table, JsonNode changeEvent, List<String> columnNames, Set<String> keyColumnNames)
      throws ChangeEventConvertorException {
    Set<String> columnNamesAsSet = new HashSet<>(columnNames);
    if (!columnNamesAsSet.containsAll(keyColumnNames)) {
      throw new ChangeEventConvertorException(
          "Missing key columns from change event. " + keyColumnNames);
    }
    Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table.name());

    for (String columnName : columnNames) {
      Type columnType = table.column(columnName).type();
      Value columnValue = null;

      String colName = columnName.toLowerCase();
      boolean requiredField = keyColumnNames.contains(columnName);
      switch (columnType.getCode()) {
        case BOOL:
        case PG_BOOL:
          columnValue =
              Value.bool(ChangeEventTypeConvertor.toBoolean(changeEvent, colName, requiredField));
          break;
        case INT64:
        case PG_INT8:
          columnValue =
              Value.int64(ChangeEventTypeConvertor.toLong(changeEvent, colName, requiredField));
          break;
        case FLOAT64:
        case PG_FLOAT8:
          columnValue =
              Value.float64(ChangeEventTypeConvertor.toDouble(changeEvent, colName, requiredField));
          break;
        case STRING:
        case PG_VARCHAR:
        case PG_TEXT:
          columnValue =
              Value.string(ChangeEventTypeConvertor.toString(changeEvent, colName, requiredField));
          break;
        case NUMERIC:
        case PG_NUMERIC:
          columnValue =
              Value.numeric(
                  ChangeEventTypeConvertor.toNumericBigDecimal(
                      changeEvent, colName, requiredField));
          break;
        case JSON:
        case PG_JSONB:
          columnValue =
              Value.string(ChangeEventTypeConvertor.toString(changeEvent, colName, requiredField));
          break;
        case BYTES:
        case PG_BYTEA:
          columnValue =
              Value.bytes(
                  ChangeEventTypeConvertor.toByteArray(changeEvent, colName, requiredField));
          break;
        case TIMESTAMP:
        case PG_COMMIT_TIMESTAMP:
        case PG_TIMESTAMPTZ:
          columnValue =
              Value.timestamp(
                  ChangeEventTypeConvertor.toTimestamp(changeEvent, colName, requiredField));
          break;
        case DATE:
        case PG_DATE:
          columnValue =
              Value.date(ChangeEventTypeConvertor.toDate(changeEvent, colName, requiredField));
          break;
          // TODO(b/179070999) - Add support for other data types.
        default:
          throw new IllegalArgumentException(
              "Column name("
                  + columnName
                  + ") has unsupported column type("
                  + columnType.getCode()
                  + ")");
      }
      builder.set(columnName).to(columnValue);
    }
    return builder;
  }
}
