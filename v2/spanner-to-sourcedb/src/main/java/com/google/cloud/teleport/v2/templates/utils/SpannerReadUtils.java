/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.changestream.DataChangeRecordTypeConvertor;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.math.BigDecimal;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;

public class SpannerReadUtils {

  public static Struct readRowAsStruct(
      DatabaseClient databaseClient,
      String tableName,
      JsonNode keysJson,
      Ddl ddl,
      List<String> columns,
      com.google.cloud.Timestamp timestamp,
      Options.RpcPriority rpcPriority)
      throws Exception {

    Key primaryKey = generateKey(tableName, keysJson, ddl);
    return readRowAsStruct(databaseClient, tableName, primaryKey, columns, timestamp, rpcPriority);
  }

  public static Struct readRowAsStruct(
      DatabaseClient databaseClient,
      String tableName,
      Key primaryKey,
      List<String> columns,
      com.google.cloud.Timestamp timestamp,
      Options.RpcPriority rpcPriority)
      throws Exception {

    try (ResultSet rs =
        databaseClient
            .singleUse(TimestampBound.ofReadTimestamp(timestamp))
            .read(
                tableName, KeySet.singleKey(primaryKey), columns, Options.priority(rpcPriority))) {
      if (!rs.next()) {
        return null;
      }
      return rs.getCurrentRowAsStruct();
    }
  }

  public static Key generateKey(String tableName, JsonNode keysJson, Ddl ddl) throws Exception {
    try {
      Table table = ddl.table(tableName);
      ImmutableList<IndexColumn> keyColumns = table.primaryKeys();
      Key.Builder pk = Key.newBuilder();

      for (IndexColumn keyColumn : keyColumns) {
        Column key = table.column(keyColumn.name());
        Type keyColType = key.type();
        String keyColName = key.name();
        switch (keyColType.getCode()) {
          case BOOL:
          case PG_BOOL:
            pk.append(
                DataChangeRecordTypeConvertor.toBoolean(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case INT64:
          case PG_INT8:
            pk.append(
                DataChangeRecordTypeConvertor.toLong(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case FLOAT32:
          case PG_FLOAT4:
            pk.append(
                DataChangeRecordTypeConvertor.toDouble(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case FLOAT64:
          case PG_FLOAT8:
            pk.append(
                DataChangeRecordTypeConvertor.toDouble(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case STRING:
          case PG_VARCHAR:
          case PG_TEXT:
            pk.append(
                DataChangeRecordTypeConvertor.toString(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case NUMERIC:
          case PG_NUMERIC:
            pk.append(
                DataChangeRecordTypeConvertor.toNumericBigDecimal(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case BYTES:
          case PG_BYTEA:
            pk.append(
                DataChangeRecordTypeConvertor.toByteArray(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case TIMESTAMP:
          case PG_TIMESTAMPTZ:
            pk.append(
                DataChangeRecordTypeConvertor.toTimestamp(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case DATE:
          case PG_DATE:
            pk.append(
                DataChangeRecordTypeConvertor.toDate(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          default:
            throw new IllegalArgumentException(
                "Column name(" + keyColName + ") has unsupported column type(" + keyColType + ")");
        }
      }
      return pk.build();
    } catch (Exception e) {
      throw new Exception("Error generating key: " + e.getMessage());
    }
  }

  public static Map<String, Object> getRowAsMap(
      Struct row, List<String> columns, String tableName, Ddl ddl) throws Exception {
    Map<String, Object> spannerRecord = new HashMap<>();
    Table table = ddl.table(tableName);
    for (String columnName : columns) {
      Column column = table.column(columnName);
      Object columnValue =
          row.isNull(columnName) ? null : getColumnValueFromRow(column, row.getValue(columnName));
      spannerRecord.put(columnName, columnValue);
    }
    return spannerRecord;
  }

  public static Object getColumnValueFromRow(Column column, Value value) throws Exception {
    try {
      Type colType = column.type();
      String colName = column.name();
      switch (colType.getCode()) {
        case BOOL:
        case PG_BOOL:
          return value.getBool();
        case INT64:
        case PG_INT8:
          return value.getInt64();
        case FLOAT32:
        case PG_FLOAT4:
          return (double) value.getFloat32();
        case FLOAT64:
        case PG_FLOAT8:
          return value.getFloat64();
        case STRING:
        case PG_VARCHAR:
        case PG_TEXT:
          return value.getString();
        case NUMERIC:
        case PG_NUMERIC:
          return value.getNumeric();
        case JSON:
        case PG_JSONB:
          return value.getString();
        case BYTES:
          return value.getBytes();
        case PG_BYTEA:
          return value.getBytesArray();
        case TIMESTAMP:
        case PG_TIMESTAMPTZ:
          return value.getTimestamp();
        case DATE:
        case PG_DATE:
          return value.getDate();
        default:
          throw new IllegalArgumentException(
              "Column name(" + colName + ") has unsupported column type(" + colType + ")");
      }
    } catch (Exception e) {
      throw new Exception("Error getting column value from row: " + e.getMessage());
    }
  }

  /*
   * Marshals Spanner's read row values to match CDC stream's representation.
   */
  public static void marshalSpannerValues(
      ObjectNode newValuesJsonNode, String tableName, String colName, Struct row, Ddl ddl) {
    if (row.isNull(colName)) {
      newValuesJsonNode.putNull(colName);
      return;
    }

    // TODO(b/430495490): Add support for string arrays on Spanner side.
    switch (ddl.table(tableName).column(colName).type().getCode()) {
      case FLOAT32:
        float val32 = row.getFloat(colName);
        if (Float.isNaN(val32) || !Float.isFinite(val32)) {
          newValuesJsonNode.put(colName, val32);

        } else {
          newValuesJsonNode.put(colName, new BigDecimal(val32));
        }
        break;
      case FLOAT64:
        double val = row.getDouble(colName);
        if (Double.isNaN(val) || !Double.isFinite(val)) {
          newValuesJsonNode.put(colName, val);

        } else {
          newValuesJsonNode.put(colName, new BigDecimal(val));
        }
        break;
      case BOOL:
        newValuesJsonNode.put(colName, row.getBoolean(colName));
        break;
      case BYTES:
        // We need to trim the base64 string to remove newlines added at the end.
        // Older version of Base64 lik e(MIME) or Privacy-Enhanced Mail (PEM), often
        // included line
        // breaks.
        // MySql adheres to RFC 4648 (the standard MySQL uses) which states that
        // implementations
        // MUST
        // NOT add line feeds to base-encoded data unless explicitly directed by a
        // referring
        // specification.
        newValuesJsonNode.put(
            colName,
            Base64.getEncoder()
                .encodeToString(row.getValue(colName).getBytes().toByteArray())
                .trim());
        break;
      default:
        newValuesJsonNode.put(colName, row.getValue(colName).toString());
    }
  }

  public static void updateColumnValues(
      TrimmedShardedDataChangeRecord record,
      String tableName,
      Ddl ddl,
      Struct row,
      Map<String, Object> rowAsMap,
      ObjectMapper mapper)
      throws Exception {
    Table table = ddl.table(tableName);
    ImmutableSet<String> keyColumns =
        table.primaryKeys().stream().map(k -> k.name()).collect(ImmutableSet.toImmutableSet());
    ObjectNode newValuesJsonNode = (ObjectNode) mapper.readTree(record.getMod().getNewValuesJson());
    rowAsMap.keySet().stream()
        .filter(k -> !keyColumns.contains(k))
        .forEach(colName -> marshalSpannerValues(newValuesJsonNode, tableName, colName, row, ddl));
    String newValuesJson = mapper.writeValueAsString(newValuesJsonNode);
    record.setMod(
        new Mod(record.getMod().getKeysJson(), record.getMod().getOldValuesJson(), newValuesJson));
  }
}
