/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerColumn;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * The {@link SpannerToBigQueryUtils} provides methods that convert Spanner types to BigQuery types.
 */
public class SpannerToBigQueryUtils {

  public static List<TableFieldSchema> spannerColumnsToBigQueryIOFields(
      List<TrackedSpannerColumn> spannerColumns) {
    final List<TableFieldSchema> bigQueryFields = new ArrayList<>(spannerColumns.size());
    for (TrackedSpannerColumn spannerColumn : spannerColumns) {
      bigQueryFields.add(spannerColumnToBigQueryIOField(spannerColumn));
    }

    return bigQueryFields;
  }

  private static TableFieldSchema spannerColumnToBigQueryIOField(
      TrackedSpannerColumn spannerColumn) {
    TableFieldSchema bigQueryField =
        new TableFieldSchema().setName(spannerColumn.getName()).setMode(Field.Mode.REPEATED.name());
    Type spannerType = spannerColumn.getType();

    if (spannerType.equals(Type.array(Type.bool()))) {
      bigQueryField.setType("BOOL");
    } else if (spannerType.equals(Type.array(Type.bytes()))) {
      bigQueryField.setType("BYTES");
    } else if (spannerType.equals(Type.array(Type.date()))) {
      bigQueryField.setType("DATE");
    } else if (spannerType.equals(Type.array(Type.float64()))) {
      bigQueryField.setType("FLOAT64");
    } else if (spannerType.equals(Type.array(Type.int64()))) {
      bigQueryField.setType("INT64");
    } else if (spannerType.equals(Type.array(Type.json()))) {
      bigQueryField.setType("STRING");
    } else if (spannerType.equals(Type.array(Type.numeric()))) {
      bigQueryField.setType("NUMERIC");
    } else if (spannerType.equals(Type.array(Type.string()))) {
      bigQueryField.setType("STRING");
    } else if (spannerType.equals(Type.array(Type.timestamp()))) {
      bigQueryField.setType("TIMESTAMP");
    } else {
      // Set NULLABLE for all non-array types, since we only insert primary key columns for deleted
      // rows, which leaves non-primary key columns always null.
      // E.g. if in Spanner schema we set "FirstName" which is non-primary key to NOT NULL, and we
      // set the same field to NOT NULL in BigQuery, when we delete the Spanner row, we will not
      // populate "FirstName" field in BigQuery, which violates the constraints.
      bigQueryField.setMode(Field.Mode.NULLABLE.name());
      StandardSQLTypeName bigQueryType;
      switch (spannerType.getCode()) {
        case BOOL:
          bigQueryType = StandardSQLTypeName.BOOL;
          break;
        case BYTES:
          bigQueryType = StandardSQLTypeName.BYTES;
          break;
        case DATE:
          bigQueryType = StandardSQLTypeName.DATE;
          break;
        case FLOAT64:
          bigQueryType = StandardSQLTypeName.FLOAT64;
          break;
        case INT64:
          bigQueryType = StandardSQLTypeName.INT64;
          break;
        case JSON:
          bigQueryType = StandardSQLTypeName.STRING;
          break;
        case NUMERIC:
          bigQueryType = StandardSQLTypeName.NUMERIC;
          break;
        case STRING:
          bigQueryType = StandardSQLTypeName.STRING;
          break;
        case TIMESTAMP:
          bigQueryType = StandardSQLTypeName.TIMESTAMP;
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported Spanner type: %s", spannerType));
      }
      bigQueryField.setType(bigQueryType.name());
    }

    return bigQueryField;
  }

  public static void spannerSnapshotRowToBigQueryTableRow(
      ResultSet resultSet, List<TrackedSpannerColumn> spannerNonPkColumns, TableRow tableRow) {
    if (resultSet.next()) {
      for (TrackedSpannerColumn spannerNonPkColumn : spannerNonPkColumns) {
        tableRow.set(
            spannerNonPkColumn.getName(),
            getColumnValueFromResultSet(spannerNonPkColumn, resultSet));
      }
    } else {
      throw new IllegalArgumentException(
          "Received zero row from the result set of Spanner snapshot row");
    }

    if (resultSet.next()) {
      throw new IllegalArgumentException(
          "Received more than one rows from the result set of Spanner snapshot row");
    }
  }

  private static Object getColumnValueFromResultSet(
      TrackedSpannerColumn spannerColumn, ResultSet resultSet) {
    String columnName = spannerColumn.getName();
    Type columnType = spannerColumn.getType();

    if (resultSet.isNull(columnName)) {
      return null;
    }

    if (columnType.equals(Type.array(Type.bool()))) {
      return resultSet.getBooleanList(columnName);
    } else if (columnType.equals(Type.array(Type.bytes()))) {
      List<ByteArray> bytesList = resultSet.getBytesList(columnName);
      List<String> result = new ArrayList<>();
      for (ByteArray bytes : bytesList) {
        result.add(bytes.toBase64());
      }
      return result;
    } else if (columnType.equals(Type.array(Type.date()))) {
      List<String> result = new ArrayList<>();
      for (Date date : resultSet.getDateList(columnName)) {
        result.add(date.toString());
      }
      return result;
    } else if (columnType.equals(Type.array(Type.float64()))) {
      return resultSet.getDoubleList(columnName);
    } else if (columnType.equals(Type.array(Type.int64()))) {
      return resultSet.getLongList(columnName);
    } else if (columnType.equals(Type.array(Type.json()))) {
      return resultSet.getJsonList(columnName);
    } else if (columnType.equals(Type.array(Type.numeric()))) {
      return resultSet.getBigDecimalList(columnName);
    } else if (columnType.equals(Type.array(Type.string()))) {
      return resultSet.getStringList(columnName);
    } else if (columnType.equals(Type.array(Type.timestamp()))) {
      List<String> result = new ArrayList<>();
      for (Timestamp timestamp : resultSet.getTimestampList(columnName)) {
        result.add(timestamp.toString());
      }
      return result;
    } else {
      Type.Code columnTypeCode = columnType.getCode();
      switch (columnTypeCode) {
        case BOOL:
          return resultSet.getBoolean(columnName);
        case BYTES:
          return resultSet.getBytes(columnName).toBase64();
        case DATE:
          return resultSet.getDate(columnName).toString();
        case FLOAT64:
          return resultSet.getDouble(columnName);
        case INT64:
          return resultSet.getLong(columnName);
        case JSON:
          return resultSet.getJson(columnName);
        case NUMERIC:
          return resultSet.getBigDecimal(columnName);
        case STRING:
          return resultSet.getString(columnName);
        case TIMESTAMP:
          return resultSet.getTimestamp(columnName).toString();
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported Spanner type: %s", columnTypeCode));
      }
    }
  }

  public static void addSpannerNonPkColumnsToTableRow(
      String newValuesJson, List<TrackedSpannerColumn> spannerNonPkColumns, TableRow tableRow) {
    JSONObject newValuesJsonObject = new JSONObject(newValuesJson);
    for (TrackedSpannerColumn spannerColumn : spannerNonPkColumns) {
      String columnName = spannerColumn.getName();
      Type columnType = spannerColumn.getType();
      if (!newValuesJsonObject.has(columnName) || newValuesJsonObject.isNull(columnName)) {
        continue;
      }

      if (columnType.equals(Type.array(Type.bool()))
          || columnType.equals(Type.array(Type.bytes()))
          || columnType.equals(Type.array(Type.date()))
          || columnType.equals(Type.array(Type.float64()))
          || columnType.equals(Type.array(Type.int64()))
          || columnType.equals(Type.array(Type.json()))
          || columnType.equals(Type.array(Type.numeric()))
          || columnType.equals(Type.array(Type.string()))
          || columnType.equals(Type.array(Type.timestamp()))) {
        JSONArray jsonArray = newValuesJsonObject.getJSONArray(columnName);
        List<Object> objects = new ArrayList<>(jsonArray.length());
        for (Object o : jsonArray) {
          objects.add(o);
        }
        tableRow.set(columnName, objects);
      } else {
        tableRow.set(columnName, newValuesJsonObject.get(columnName));
      }
    }
  }
}
