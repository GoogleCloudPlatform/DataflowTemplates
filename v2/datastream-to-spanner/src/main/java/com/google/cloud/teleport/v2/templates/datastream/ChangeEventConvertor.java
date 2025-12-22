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
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.constants.DatastreamConstants;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventSpannerConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ChangeEventUtils;
import com.google.common.collect.ImmutableList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Helper class with static methods that convert Change Events to Cloud Spanner mutations. */
public class ChangeEventConvertor {

  private ChangeEventConvertor() {}

  static void verifySpannerSchema(Ddl ddl, JsonNode changeEvent)
      throws ChangeEventConvertorException, InvalidChangeEventException, DroppedTableException {
    String tableName = changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText();
    if (ddl.table(tableName) == null) {
      throw new DroppedTableException(
          "Table from change event does not exist in Spanner. table=" + tableName);
    }
    List<String> changeEventColumns = ChangeEventUtils.getEventColumnKeys(changeEvent);
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
    List<String> changeEventKeys = ChangeEventUtils.getEventColumnKeys(changeEvent);
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

      return ChangeEventSpannerConvertor.mutationBuilderFromEvent(
          table.name(),
          table,
          changeEvent,
          keyColumnNames,
          requiredKeyColumnNames,
          /* convertNameToLowerCase= */ true);
    } catch (Exception e) {
      throw new ChangeEventConvertorException(e);
    }
  }

  private static Mutation changeEventToInsertOrUpdateMutation(Ddl ddl, JsonNode changeEvent)
      throws ChangeEventConvertorException, InvalidChangeEventException {
    String tableName = changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText();
    List<String> changeEventKeys = ChangeEventUtils.getEventColumnKeys(changeEvent);
    try {
      Table table = ddl.table(tableName);

      Set<String> keyColumns =
          table.primaryKeys().stream()
              .map(keyCol -> keyCol.name())
              .map(colName -> colName.toLowerCase())
              .collect(Collectors.toSet());
      return ChangeEventSpannerConvertor.mutationFromEvent(
          table, changeEvent, changeEventKeys, keyColumns);
    } catch (Exception e) {
      throw new ChangeEventConvertorException(e);
    }
  }

  private static Mutation changeEventToDeleteMutation(Ddl ddl, JsonNode changeEvent)
      throws ChangeEventConvertorException {
    String tableName = changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText();
    com.google.cloud.spanner.Key primaryKey =
        ChangeEventSpannerConvertor.changeEventToPrimaryKey(
            tableName, ddl, changeEvent, /* convertNameToLowerCase= */ true);
    try {
      return Mutation.delete(tableName, primaryKey);
    } catch (Exception e) {
      throw new ChangeEventConvertorException(e);
    }
  }
}
