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
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.constants.DatastreamConstants;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventSpannerConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import com.google.cloud.teleport.v2.spanner.migrations.spanner.SpannerReadUtils;
import com.google.cloud.teleport.v2.templates.spanner.ShadowTableCreator;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
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

    this.primaryKey =
        ChangeEventSpannerConvertor.changeEventToPrimaryKey(
            changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText(),
            ddl,
            changeEvent,
            /* convertNameToLowerCase= */ true);
    this.dataMutation = ChangeEventConvertor.changeEventToMutation(ddl, changeEvent);
    this.shadowTableMutation = generateShadowTableMutation(ddl, shadowTableDdl);
  }

  public JsonNode getChangeEvent() {
    return changeEvent;
  }

  // Returns an array of data and shadow table mutations.
  public Iterable<Mutation> getMutations() {
    return Arrays.asList(dataMutation, shadowTableMutation);
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
}
