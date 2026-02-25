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
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventSpannerConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventTypeConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import com.google.cloud.teleport.v2.spanner.migrations.spanner.SpannerReadUtils;
import com.google.cloud.teleport.v2.templates.spanner.ShadowTableCreator;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ChangeEventContext class converts change events to Cloud Spanner mutations and stores all
 * intermediary objects prior to applying them to Cloud Spanner.
 */
public abstract class ChangeEventContext {

  private static final Logger LOG = LoggerFactory.getLogger(ChangeEventContext.class);

  // The JsonNode representation of the change event.
  protected JsonNode changeEvent;

  // Cloud Spanner mutation for the change event.
  protected Mutation dataMutation;

  // Cloud Spanner primary key for the change event.
  protected Key primaryKey;

  // Shadow table for the change event.
  protected String shadowTable;

  // Cloud Spanner mutation for the shadow table corresponding to this change event.
  protected Mutation shadowTableMutation;

  // The prefix to be applied for shadow table.
  protected String shadowTablePrefix;

  // Data table for the change event.
  protected String dataTable;

  // Immutable map to store the "safe" column names of the shadow table (to avoid collision with
  // data column names). <key: metadata name, value: safe shadow col name>
  protected final ImmutableMap<String, String> safeShadowColNames;

  protected ChangeEventContext(
      JsonNode changeEvent, Ddl ddl, Map<String, Pair<String, String>> shadowColumnConstants)
      throws InvalidChangeEventException, DroppedTableException {
    this.changeEvent = changeEvent;
    this.dataTable = changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText();
    Table table = ddl.table(this.dataTable);
    if (table == null) {
      throw new DroppedTableException(
          "Table from change event does not exist in Spanner. table=" + this.dataTable);
    }
    Set<String> existingPrimaryKeyColumnNames =
        table.primaryKeys().stream().map(k -> k.name()).collect(Collectors.toSet());

    ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
    for (Map.Entry<String, Pair<String, String>> entry : shadowColumnConstants.entrySet()) {
      String metadataKey = entry.getKey();
      String originalShadowName = entry.getValue().getLeft();
      String safeName =
          ShadowTableCreator.getSafeShadowColumnName(
              originalShadowName, existingPrimaryKeyColumnNames);
      mapBuilder.put(metadataKey, safeName);
    }
    this.safeShadowColNames = mapBuilder.build();
  }

  // Abstract method to generate shadow table mutation.
  abstract Mutation generateShadowTableMutation(Ddl ddl, Ddl shadowDdl)
      throws ChangeEventConvertorException;

  // Helper method to convert change event to mutation.
  protected void convertChangeEventToMutation(Ddl ddl, Ddl shadowTableDdl)
      throws ChangeEventConvertorException, InvalidChangeEventException, DroppedTableException {
    ChangeEventConvertor.convertChangeEventColumnKeysToLowerCase(changeEvent);
    ChangeEventConvertor.verifySpannerSchema(ddl, changeEvent);

    boolean hasGeneratedPK = false;
    Table table = ddl.table(this.dataTable);
    if (table != null) {
      hasGeneratedPK = hasGeneratedPK(table);
    }

    this.primaryKey =
        ChangeEventSpannerConvertor.changeEventToPrimaryKey(
            changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText(),
            ddl,
            changeEvent,
            /* convertNameToLowerCase= */ true);

    String changeType = getChangeType(changeEvent);
    boolean isDelete = DatastreamConstants.DELETE_EVENT.equalsIgnoreCase(changeType);

    if (hasGeneratedPK && isDelete) {
      // For delete events on tables with generated primary keys, we need to use DML
      // to delete the row.
      this.dataMutation = null;
    } else {
      this.dataMutation = ChangeEventConvertor.changeEventToMutation(ddl, changeEvent);
    }

    this.shadowTableMutation = generateShadowTableMutation(ddl, shadowTableDdl);
  }

  public Statement getDataDmlStatement(Ddl ddl) throws ChangeEventConvertorException {
    String changeType = getChangeType(changeEvent);
    boolean isDelete = DatastreamConstants.DELETE_EVENT.equalsIgnoreCase(changeType);
    if (!isDelete) {
      return null;
    }
    Table table = ddl.table(this.dataTable);
    if (table != null && hasGeneratedPK(table)) {
      return generateDeleteDml(table, this.dataTable, changeEvent);
    }
    return null;
  }

  private boolean hasGeneratedPK(Table table) {
    for (IndexColumn keyColumn : table.primaryKeys()) {
      if (table.column(keyColumn.name()).isGenerated()) {
        return true;
      }
    }
    return false;
  }

  private Statement generateDeleteDml(Table table, String tableName, JsonNode event)
      throws ChangeEventConvertorException {
    // TODO: Add support for PostgreSQL
    StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableName).append(" WHERE ");
    Statement.Builder builder = Statement.newBuilder("");
    boolean first = true;
    for (com.google.cloud.teleport.v2.spanner.ddl.Column column : table.columns()) {
      String colName = column.name();
      if (column.isGenerated()) {
        continue;
      }
      if (!first) {
        sql.append(" AND ");
      }
      sql.append(colName).append(" = @").append(colName);
      // Bind value
      Value value =
          ChangeEventTypeConvertor.toValue(event, column.type(), colName, /* requiredField */ true);
      builder.bind(colName).to(value);
      first = false;
    }
    builder.replace(sql.toString());
    return builder.build();
  }

  public JsonNode getChangeEvent() {
    return changeEvent;
  }

  // Returns an array of data and shadow table mutations.
  public Iterable<Mutation> getMutations() {
    List<Mutation> mutations = new ArrayList<>();
    if (dataMutation != null) {
      mutations.add(dataMutation);
    }
    if (shadowTableMutation != null) {
      mutations.add(shadowTableMutation);
    }
    return mutations;
  }

  // Returns the data table mutation
  public Mutation getDataMutation() {
    return dataMutation;
  }

  // Returns the shadow table mutation
  public Mutation getShadowMutation() {
    return shadowTableMutation;
  }

  // Getter method for the primary key of the change event.
  public Key getPrimaryKey() {
    return primaryKey;
  }

  // Getter method for shadow mutation (used for tests).
  public Mutation getShadowTableMutation() {
    return shadowTableMutation;
  }

  // Getter method for the shadow table.
  public String getShadowTable() {
    return shadowTable;
  }

  // Getter for the safe shadow table column names.
  public String getSafeShadowColumn(String key) {
    return safeShadowColNames.get(key);
  }

  // Fires a read on Data table with lock scanned ranges. Used to acquire exclusive lock on Data row
  // at the beginning of a readWriteTransaction
  public void readDataTableRowWithExclusiveLock(
      final TransactionContext transactionContext, Ddl dataTableDdl) {
    // TODO: After beam release, use the latest client lib version which supports setting lock
    // hints via the read api. SQL string generation should be removed.
    List<String> columnNames =
        dataTableDdl.table(dataTable).primaryKeys().stream()
            .map(col -> col.name())
            .collect(Collectors.toList());
    Statement sql =
        SpannerReadUtils.generateReadSQLWithExclusiveLock(
            dataTable, columnNames, primaryKey, dataTableDdl);
    ResultSet resultSet = transactionContext.executeQuery(sql);
    if (!resultSet.next()) {
      return;
    }
    // Read the row in order to acquire the lock and discard it.
    resultSet.getCurrentRowAsStruct();
  }

  public static String getChangeType(JsonNode changeEvent) {
    return changeEvent.has(DatastreamConstants.EVENT_CHANGE_TYPE_KEY)
        ? changeEvent
            .get(DatastreamConstants.EVENT_CHANGE_TYPE_KEY)
            .asText(DatastreamConstants.EMPTY_EVENT)
        : DatastreamConstants.EMPTY_EVENT;
  }
}
