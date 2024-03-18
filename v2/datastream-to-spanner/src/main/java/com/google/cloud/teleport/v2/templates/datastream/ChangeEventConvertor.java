/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.templates.datastream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventTypeConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventTypeConvertorException;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.common.collect.ImmutableList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Helper class with static methods that convert Change Events to Cloud Spanner mutations. */
public class ChangeEventConvertor {

  private ChangeEventConvertor() {}

  static void verifySpannerSchema(Ddl ddl, JsonNode changeEvent)
      throws ChangeEventConvertorException, InvalidChangeEventException {
    String tableName = changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText();
    if (ddl.table(tableName) == null) {
      throw new ChangeEventConvertorException(
          "Table from change event does not exist in Spanner. table=" + tableName);
    }
    List<String> changeEventColumns = getEventColumnKeys(changeEvent);
    for (String changeEventColumn : changeEventColumns) {
      if (ddl.table(tableName).column(changeEventColumn) == null) {
        throw new ChangeEventConvertorException(
            "Column from change event doesn't exist in Spanner. column="
                + changeEventColumn
                + ", table="
                + tableName);
      }
    }
    Set<String> keyColumns =
        ddl.table(tableName).primaryKeys().stream()
            .map(keyCol -> keyCol.name())
            .map(colName -> colName.toLowerCase())
            .collect(Collectors.toSet());
    Set<String> changeEventColumnsAsSet = new HashSet<>(changeEventColumns);
    if (!changeEventColumnsAsSet.containsAll(keyColumns)) {
      throw new ChangeEventConvertorException(
          "Key columns from change event do not exist in Spanner. keyColumns=" + keyColumns);
    }
  }

  static void convertChangeEventColumnKeysToLowerCase(JsonNode changeEvent)
      throws ChangeEventConvertorException, InvalidChangeEventException {
    List<String> changeEventKeys = getEventColumnKeys(changeEvent);
    ObjectNode jsonNode = (ObjectNode) changeEvent;
    for (String key : changeEventKeys) {
      // Skip keys that are in lower case.
      if (key.equals(key.toLowerCase())) {
        continue;
      }
      jsonNode.set(key.toLowerCase(), changeEvent.get(key));
      jsonNode.remove(key);
    }
  }

  static Mutation changeEventToMutation(Ddl ddl, JsonNode changeEvent)
      throws ChangeEventConvertorException, InvalidChangeEventException {
    String changeType =
        changeEvent.has(DatastreamConstants.EVENT_CHANGE_TYPE_KEY)
            ? changeEvent
                .get(DatastreamConstants.EVENT_CHANGE_TYPE_KEY)
                .asText(DatastreamConstants.EMPTY_EVENT)
            : DatastreamConstants.EMPTY_EVENT;
    /* DDL events are not handled and hence skipped.
     * The following block is to ensure that we only handle
     * a) Change events that are DML operations.
     * b) DUMP events. Data which existed in the source db before the start
     * of the Stream creation are replicated as DUMP events. They do not have
     * change type.
     */
    if (!(DatastreamConstants.DELETE_EVENT.equalsIgnoreCase(changeType)
        || DatastreamConstants.INSERT_EVENT.equalsIgnoreCase(changeType)
        || DatastreamConstants.UPDATE_EVENT.equalsIgnoreCase(changeType)
        || DatastreamConstants.UPDATE_INSERT_EVENT.equalsIgnoreCase(changeType)
        || DatastreamConstants.UPDATE_DELETE_EVENT.equalsIgnoreCase(changeType)
        || DatastreamConstants.EMPTY_EVENT.equalsIgnoreCase(changeType))) {
      throw new InvalidChangeEventException("Unexpected event with change type " + changeType);
    }

    if (DatastreamConstants.DELETE_EVENT.equalsIgnoreCase(changeType)
        || DatastreamConstants.UPDATE_DELETE_EVENT.equalsIgnoreCase(changeType)) {
      return ChangeEventConvertor.changeEventToDeleteMutation(ddl, changeEvent);
    }
    // Dump events, Insert events and  Update events  are treated the same way.
    return ChangeEventConvertor.changeEventToInsertOrUpdateMutation(ddl, changeEvent);
  }

  static Mutation.WriteBuilder changeEventToShadowTableMutationBuilder(
      Ddl ddl, JsonNode changeEvent, String shadowTablePrefix)
      throws ChangeEventConvertorException {
    String tableName = changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText();
    String shadowTableName = shadowTablePrefix + tableName;
    try {
      Table table = ddl.table(shadowTableName);
      ImmutableList<IndexColumn> keyColumns = table.primaryKeys();
      List<String> keyColumnNames =
          keyColumns.stream()
              .map(IndexColumn::name)
              .map(colName -> colName.toLowerCase())
              .collect(Collectors.toList());
      Set<String> requiredKeyColumnNames = new HashSet<>(keyColumnNames);
      Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(shadowTableName);

      populateMutationBuilderWithEvent(
          table, builder, changeEvent, keyColumnNames, requiredKeyColumnNames);
      return builder;
    } catch (Exception e) {
      throw new ChangeEventConvertorException(e);
    }
  }

  static com.google.cloud.spanner.Key changeEventToPrimaryKey(Ddl ddl, JsonNode changeEvent)
      throws ChangeEventConvertorException {
    String tableName = changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText();
    try {
      Table table = ddl.table(tableName);
      ImmutableList<IndexColumn> keyColumns = table.primaryKeys();
      com.google.cloud.spanner.Key.Builder pk = com.google.cloud.spanner.Key.newBuilder();

      for (IndexColumn keyColumn : keyColumns) {
        Column key = table.column(keyColumn.name());
        Type keyColType = key.type();
        String keyColName = key.name().toLowerCase();
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

  private static Mutation changeEventToInsertOrUpdateMutation(Ddl ddl, JsonNode changeEvent)
      throws ChangeEventConvertorException, InvalidChangeEventException {
    String tableName = changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText();
    List<String> changeEventKeys = getEventColumnKeys(changeEvent);
    try {
      Table table = ddl.table(tableName);
      Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table.name());

      Set<String> keyColumns =
          table.primaryKeys().stream()
              .map(keyCol -> keyCol.name())
              .map(colName -> colName.toLowerCase())
              .collect(Collectors.toSet());
      populateMutationBuilderWithEvent(table, builder, changeEvent, changeEventKeys, keyColumns);

      return builder.build();
    } catch (Exception e) {
      throw new ChangeEventConvertorException(e);
    }
  }

  private static Mutation changeEventToDeleteMutation(Ddl ddl, JsonNode changeEvent)
      throws ChangeEventConvertorException {
    String tableName = changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText();
    com.google.cloud.spanner.Key primaryKey =
        ChangeEventConvertor.changeEventToPrimaryKey(ddl, changeEvent);
    try {
      return Mutation.delete(tableName, primaryKey);
    } catch (Exception e) {
      throw new ChangeEventConvertorException(e);
    }
  }

  private static void populateMutationBuilderWithEvent(
      Table table,
      Mutation.WriteBuilder builder,
      JsonNode changeEvent,
      List<String> columnNames,
      Set<String> keyColumnNames)
      throws ChangeEventConvertorException, ChangeEventTypeConvertorException {
    Set<String> columnNamesAsSet = new HashSet<>(columnNames);
    if (!columnNamesAsSet.containsAll(keyColumnNames)) {
      throw new ChangeEventConvertorException(
          "Missing key columns from change event. " + keyColumnNames);
    }
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
  }

  private static List<String> getEventColumnKeys(JsonNode changeEvent)
      throws InvalidChangeEventException {
    // Filter all keys which have the metadata prefix
    Iterator<String> fieldNames = changeEvent.fieldNames();
    List<String> eventColumnKeys =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(fieldNames, Spliterator.ORDERED), false)
            .filter(f -> !f.startsWith(DatastreamConstants.EVENT_METADATA_KEY_PREFIX))
            .collect(Collectors.toList());
    if (eventColumnKeys.size() == 0) {
      throw new InvalidChangeEventException("No data found in Datastream event. ");
    }
    return eventColumnKeys;
  }
}
