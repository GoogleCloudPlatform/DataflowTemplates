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
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.spanner.type.Type.Code;
import com.google.common.collect.ImmutableList;
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
    return mutationBuilderFromEvent(
            table.name(), table, changeEvent, columnNames, keyColumnNames, true)
        .build();
  }

  public static Mutation.WriteBuilder mutationBuilderFromEvent(
      String tableName,
      Table table,
      JsonNode changeEvent,
      List<String> columnNames,
      Set<String> keyColumnNames,
      boolean convertNameToLowerCase)
      throws ChangeEventConvertorException {
    Set<String> columnNamesAsSet = new HashSet<>(columnNames);
    if (!columnNamesAsSet.containsAll(keyColumnNames)) {
      throw new ChangeEventConvertorException(
          "Missing key columns from change event. " + keyColumnNames);
    }
    Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(tableName);

    for (String columnName : columnNames) {
      Column col = table.column(columnName);
      if (col.isGenerated()) {
        continue;
      }
      Type columnType = col.type();

      String colName = convertNameToLowerCase ? columnName.toLowerCase() : columnName;
      boolean requiredField = keyColumnNames.contains(columnName);
      Value columnValue =
          ChangeEventTypeConvertor.toValue(changeEvent, columnType, colName, requiredField);
      // TODO(b/422928714): Add support for Array types.
      if (columnType.getCode() != Code.ARRAY) {
        builder.set(columnName).to(columnValue);
      }
    }
    return builder;
  }

  public static com.google.cloud.spanner.Key changeEventToPrimaryKey(
      String tableName, Ddl ddl, JsonNode changeEvent, boolean convertNameToLowerCase)
      throws ChangeEventConvertorException {
    try {
      Table table = ddl.table(tableName);
      ImmutableList<IndexColumn> keyColumns = table.primaryKeys();
      com.google.cloud.spanner.Key.Builder pk = com.google.cloud.spanner.Key.newBuilder();

      for (IndexColumn keyColumn : keyColumns) {
        Column key = table.column(keyColumn.name());
        Type keyColType = key.type();
        String keyColName = convertNameToLowerCase ? key.name().toLowerCase() : key.name();
        switch (keyColType.getCode()) {
          case BOOL:
          case PG_BOOL:
            pk.append(
                ChangeEventTypeConvertor.toBoolean(
                    changeEvent, keyColName, /* requiredField= */ true));
            break;
          case INT64:
          case PG_INT8:
            pk.append(
                ChangeEventTypeConvertor.toLong(
                    changeEvent, keyColName, /* requiredField= */ true));
            break;
          case FLOAT64:
          case PG_FLOAT8:
            pk.append(
                ChangeEventTypeConvertor.toDouble(
                    changeEvent, keyColName, /* requiredField= */ true));
            break;
          case STRING:
          case PG_VARCHAR:
          case PG_TEXT:
            pk.append(
                ChangeEventTypeConvertor.toString(
                    changeEvent, keyColName, /* requiredField= */ true));
            break;
          case NUMERIC:
          case PG_NUMERIC:
            pk.append(
                ChangeEventTypeConvertor.toNumericBigDecimal(
                    changeEvent, keyColName, /* requiredField= */ true));
            break;
          case JSON:
          case PG_JSONB:
            pk.append(
                ChangeEventTypeConvertor.toString(
                    changeEvent, keyColName, /* requiredField= */ true));
            break;
          case BYTES:
          case PG_BYTEA:
            pk.append(
                ChangeEventTypeConvertor.toByteArray(
                    changeEvent, keyColName, /* requiredField= */ true));
            break;
          case TIMESTAMP:
          case PG_COMMIT_TIMESTAMP:
          case PG_TIMESTAMPTZ:
            pk.append(
                ChangeEventTypeConvertor.toTimestamp(
                    changeEvent, keyColName, /* requiredField= */ true));
            break;
          case DATE:
          case PG_DATE:
            pk.append(
                ChangeEventTypeConvertor.toDate(
                    changeEvent, keyColName, /* requiredField= */ true));
            break;
            // TODO(b/179070999) -  Add support for other data types.
          default:
            throw new IllegalArgumentException(
                "Column name(" + keyColName + ") has unsupported column type(" + keyColType + ")");
        }
      }
      return pk.build();
    } catch (Exception e) {
      throw new ChangeEventConvertorException(e);
    }
  }
}
