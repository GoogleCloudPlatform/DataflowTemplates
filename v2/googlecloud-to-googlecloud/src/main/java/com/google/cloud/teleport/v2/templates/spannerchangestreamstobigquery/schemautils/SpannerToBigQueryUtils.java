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
import com.google.cloud.bigquery.Field;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerColumn;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link SpannerToBigQueryUtils} provides methods that convert Spanner types to BigQuery types.
 */
public class SpannerToBigQueryUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerToBigQueryUtils.class);

  // Convert user columns in TableRow to BigQueryIO TableFieldSchema.
  public static List<TableFieldSchema> tableRowColumnsToBigQueryIOFields(
      TableRow tableRow, boolean useStorageWriteApi) {
    // Filter out the metadataColumns in building TableFieldSchema for user column tables in
    // tableRowColumnsToBigQueryIOFields().
    HashSet<String> metadataColumns =
        new HashSet<>(
            Arrays.asList(
                BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_MOD_TYPE,
                BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_TABLE_NAME,
                BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_SPANNER_COMMIT_TIMESTAMP,
                BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_SERVER_TRANSACTION_ID,
                BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_RECORD_SEQUENCE,
                BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION,
                BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_NUMBER_OF_RECORDS_IN_TRANSACTION,
                BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_NUMBER_OF_PARTITIONS_IN_TRANSACTION));
    if (!useStorageWriteApi) {
      metadataColumns.add(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_BIGQUERY_COMMIT_TIMESTAMP);
    }
    List<TableFieldSchema> bigQueryFields = new ArrayList<>();
    for (String colName : tableRow.keySet()) {
      // Only create TableFieldSchema for user columns which has type information stored in the
      // TableRow as well.
      if (!metadataColumns.contains(colName) && tableRow.containsKey("_type_" + colName)) {
        bigQueryFields.add(
            tableRowColumnsToBigQueryIOField(colName, (String) tableRow.get("_type_" + colName)));
      }
    }
    return bigQueryFields;
  }

  private static void setBigQueryFieldType(
      Set<String> supportedTypes, TableFieldSchema bigQueryField, String type) {
    if (supportedTypes.contains(type)) {
      bigQueryField.setType(type);
      if (type.equals("PG_NUMERIC")) {
        bigQueryField.setType("STRING");
      } else if (type.equals("PG_JSONB")) {
        bigQueryField.setType("JSON");
      }
    } else {
      throw new IllegalArgumentException(String.format("Unsupported Spanner type: %s", type));
    }
  }

  private static TableFieldSchema tableRowColumnsToBigQueryIOField(String name, String type) {
    TableFieldSchema bigQueryField =
        new TableFieldSchema().setName(name).setMode(Field.Mode.REPEATED.name());
    String[] supportedTypesArr =
        new String[] {
          "BOOL",
          "BYTES",
          "DATE",
          "FLOAT64",
          "INT64",
          "JSON",
          "NUMERIC",
          "PG_NUMERIC",
          "PG_JSONB",
          "STRING",
          "TIMESTAMP"
        };
    HashSet<String> supportedTypes = new HashSet<>(Arrays.asList(supportedTypesArr));
    if (type.startsWith("ARRAY")) {
      String arrayItemType = type.substring(6, type.length() - 1);
      if (supportedTypes.contains(arrayItemType)) {
        if (arrayItemType.equals("PG_NUMERIC")) {
          bigQueryField.setType("STRING");
        } else if (arrayItemType.equals("PG_JSONB")) {
          bigQueryField.setType("JSON");
        } else {
          bigQueryField.setType(arrayItemType);
        }
      } else {
        throw new IllegalArgumentException(
            String.format("Unsupported Spanner type: %s", arrayItemType));
      }
    } else {
      // Set NULLABLE for all non-array types, since we only insert primary key columns for deleted
      // rows, which leaves non-primary key columns always null.
      // E.g. if in Spanner schema we set "FirstName" which is non-primary key to NOT NULL, and we
      // set the same field to NOT NULL in BigQuery, when we delete the Spanner row, we will not
      // populate "FirstName" field in BigQuery, which violates the constraints.
      bigQueryField.setMode(Field.Mode.NULLABLE.name());
      if (supportedTypes.contains(type)) {
        if (type.equals("PG_NUMERIC")) {
          bigQueryField.setType("STRING");
        } else if (type.equals("PG_JSONB")) {
          bigQueryField.setType("JSON");
        } else {
          bigQueryField.setType(type);
        }
      } else {
        throw new IllegalArgumentException(String.format("Unsupported Spanner type: %s", type));
      }
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

    // All the NULL columns in the array are filtered out since BigQuery doesn't allow NULL in the
    // array.
    if (columnType.equals(Type.array(Type.bool()))) {
      return removeNulls(resultSet.getBooleanList(columnName));
    } else if (columnType.equals(Type.array(Type.bytes()))) {
      return removeNulls(resultSet.getBytesList(columnName)).stream()
          .map(e -> e.toBase64())
          .collect(Collectors.toList());
    } else if (columnType.equals(Type.array(Type.date()))) {
      return removeNulls(resultSet.getDateList(columnName)).stream()
          .map(e -> e.toString())
          .collect(Collectors.toList());
    } else if (columnType.equals(Type.array(Type.float64()))) {
      return removeNulls(resultSet.getDoubleList(columnName));
    } else if (columnType.equals(Type.array(Type.int64()))) {
      return removeNulls(resultSet.getLongList(columnName));
    } else if (columnType.equals(Type.array(Type.json()))) {
      return removeNulls(resultSet.getJsonList(columnName));
    } else if (columnType.equals(Type.array(Type.numeric()))) {
      return removeNulls(resultSet.getBigDecimalList(columnName));
    } else if (columnType.equals(Type.array(Type.pgNumeric()))) {
      return removeNulls(resultSet.getStringList(columnName));
    } else if (columnType.equals(Type.array(Type.pgJsonb()))) {
      return removeNulls(resultSet.getPgJsonbList(columnName));
    } else if (columnType.equals(Type.array(Type.string()))) {
      return removeNulls(resultSet.getStringList(columnName));
    } else if (columnType.equals(Type.array(Type.timestamp()))) {
      return removeNulls(resultSet.getTimestampList(columnName)).stream()
          .map(e -> e.toString())
          .collect(Collectors.toList());
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
        case PG_NUMERIC:
          return resultSet.getString(columnName);
        case PG_JSONB:
          return resultSet.getPgJsonb(columnName);
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

  private static <T> List<T> removeNulls(List<T> list) {
    return list.stream().filter(Objects::nonNull).collect(Collectors.toList());
  }

  public static void addSpannerPkColumnsToTableRow(
      JSONObject keysJsonObject, List<TrackedSpannerColumn> spannerPkColumns, TableRow tableRow) {
    for (TrackedSpannerColumn spannerColumn : spannerPkColumns) {
      String spannerColumnName = spannerColumn.getName();
      if (keysJsonObject.has(spannerColumnName)) {
        tableRow.set(spannerColumnName, keysJsonObject.get(spannerColumnName));
        tableRow.set("_type_" + spannerColumnName, spannerColumn.getType().toString());
      } else {
        throw new IllegalArgumentException("Cannot find value for key column " + spannerColumnName);
      }
    }
  }

  public static String cleanSpannerType(String typeStr) {
    // Remove type annotation, e.g. NUMERIC<PG_NUMERIC> -> NUMERIC; ARRAY<NUMERIC<PG_NUMERIC>> ->
    // ARRAY<NUMERIC>
    if (typeStr.startsWith("ARRAY")) {
      String arrayItemType = typeStr.substring(6, typeStr.length() - 1);
      return "ARRAY<" + cleanSpannerType(arrayItemType) + ">";
    } else {
      int idx = typeStr.indexOf("<");
      if (idx != -1) {
        typeStr = typeStr.substring(0, idx);
      }
    }
    return typeStr;
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
          || columnType.equals(Type.array(Type.pgNumeric()))
          || columnType.equals(Type.array(Type.pgJsonb()))
          || columnType.equals(Type.array(Type.string()))
          || columnType.equals(Type.array(Type.timestamp()))) {
        JSONArray jsonArray = newValuesJsonObject.getJSONArray(columnName);
        List<Object> objects = new ArrayList<>(jsonArray.length());
        for (int i = 0; i < jsonArray.length(); i++) {
          // BigQuery array doesn't allow NULL values.
          if (!jsonArray.isNull(i)) {
            objects.add(jsonArray.get(i));
          }
        }
        tableRow.set(columnName, objects);
        tableRow.set("_type_" + columnName, cleanSpannerType(columnType.toString()));
      } else {
        tableRow.set(columnName, newValuesJsonObject.get(columnName));
        tableRow.set("_type_" + columnName, cleanSpannerType(columnType.toString()));
      }
    }
  }
}
