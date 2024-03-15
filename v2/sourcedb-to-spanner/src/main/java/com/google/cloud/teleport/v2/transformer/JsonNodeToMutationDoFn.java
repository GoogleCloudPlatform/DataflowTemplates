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
package com.google.cloud.teleport.v2.transformer;

import static com.google.cloud.teleport.v2.constants.Constants.EVENT_TABLE_NAME_KEY;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SyntheticPKey;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms a {@link JsonNode} to a {@link Mutation}. Apply transformations based on the schema
 * object in the process.
 */
public class JsonNodeToMutationDoFn extends DoFn<JsonNode, Mutation> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(JsonNodeToMutationDoFn.class);

  private Schema schema;
  private String srcTable;
  private Set<String> columnsToIgnore;

  private final PCollectionView<Ddl> ddlView;

  private final Counter mutationBuildErrors =
      Metrics.counter(JsonNodeToMutationDoFn.class, "mutationBuildErrors");

  // Jackson Object mapper.
  private transient ObjectMapper mapper;

  public JsonNodeToMutationDoFn(
      Schema schema, String srcTable, Set<String> columnsToIgnore, PCollectionView<Ddl> ddlView) {
    this.schema = schema;
    this.srcTable = srcTable;
    this.columnsToIgnore = columnsToIgnore == null ? Collections.emptySet() : columnsToIgnore;
    this.ddlView = ddlView;
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    JsonNode changeEvent = c.element();
    LOG.info(changeEvent.toPrettyString());
    Ddl ddl = c.sideInput(ddlView);
    try {
      verifyTableInSession(srcTable);
      changeEvent = transformChangeEventViaSessionFile(changeEvent);
      // TODO: add handling for numeric data in json.
      LOG.info(changeEvent.toPrettyString());

      String spTableName = changeEvent.get(EVENT_TABLE_NAME_KEY).asText();
      List<String> columnNames = schema.getSpannerColumnNames(spTableName);
      Set<String> keyColumns = schema.getPrimaryKeySet(spTableName);
      Table table = ddl.table(spTableName);

      Mutation.WriteBuilder mutation = Mutation.newInsertOrUpdateBuilder(spTableName);
      populateMutationBuilderWithEvent(table, mutation, changeEvent, columnNames, keyColumns);
      c.output(mutation.build());
    } catch (ChangeEventConvertorException ex) {
      mutationBuildErrors.inc();
      LOG.error(ex.toString());
    }
  }

  void verifyTableInSession(String tableName) throws IllegalArgumentException {
    if (!schema.getSrcToID().containsKey(tableName)) {
      throw new IllegalArgumentException(
          "Missing entry for " + tableName + " in srcToId map, provide a valid session file.");
    }
    if (!schema.getToSpanner().containsKey(tableName)) {
      throw new IllegalArgumentException(
          "Cannot find entry for "
              + tableName
              + " in toSpanner map, it is likely this table was dropped");
    }
    String tableId = schema.getSrcToID().get(tableName).getName();
    if (!schema.getSpSchema().containsKey(tableId)) {
      throw new IllegalArgumentException(
          "Missing entry for " + tableId + " in spSchema, provide a valid session file.");
    }
  }

  /**
   * This function modifies the change event using transformations based on the session file (stored
   * in the Schema object). This includes column/table name changes and adding of synthetic Primary
   * Keys.
   */
  JsonNode transformChangeEventViaSessionFile(JsonNode changeEvent) {
    String tableName = changeEvent.get(EVENT_TABLE_NAME_KEY).asText();
    String tableId = schema.getSrcToID().get(tableName).getName();

    // Convert table and column names in change event.
    changeEvent = convertTableAndColumnNames(changeEvent, tableName);

    // Add synthetic PK to change event.
    changeEvent = addSyntheticPKs(changeEvent, tableId);

    // Remove columns present in change event that were dropped in Spanner.
    changeEvent = removeDroppedColumns(changeEvent, tableId);

    return changeEvent;
  }

  JsonNode convertTableAndColumnNames(JsonNode changeEvent, String tableName) {
    NameAndCols nameAndCols = schema.getToSpanner().get(tableName);
    String spTableName = nameAndCols.getName();
    Map<String, String> cols = nameAndCols.getCols();

    // Convert the table name to corresponding Spanner table name.
    ((ObjectNode) changeEvent).put(EVENT_TABLE_NAME_KEY, spTableName);
    // Convert the column names to corresponding Spanner column names.
    for (Map.Entry<String, String> col : cols.entrySet()) {
      String srcCol = col.getKey(), spCol = col.getValue();
      if (!srcCol.equals(spCol)) {
        ((ObjectNode) changeEvent).set(spCol, changeEvent.get(srcCol));
        ((ObjectNode) changeEvent).remove(srcCol);
      }
    }
    return changeEvent;
  }

  JsonNode addSyntheticPKs(JsonNode changeEvent, String tableId) {
    Map<String, SpannerColumnDefinition> spCols = schema.getSpSchema().get(tableId).getColDefs();
    Map<String, SyntheticPKey> synthPks = schema.getSyntheticPks();
    if (synthPks.containsKey(tableId)) {
      String colID = synthPks.get(tableId).getColId();
      if (!spCols.containsKey(colID)) {
        throw new IllegalArgumentException(
            "Missing entry for "
                + colID
                + " in colDefs for tableId: "
                + tableId
                + ", provide a valid session file.");
      }
      ((ObjectNode) changeEvent).put(spCols.get(colID).getName(), UUID.randomUUID().toString());
    }
    return changeEvent;
  }

  JsonNode removeDroppedColumns(JsonNode changeEvent, String tableId) {
    Map<String, SpannerColumnDefinition> spCols = schema.getSpSchema().get(tableId).getColDefs();
    SourceTable srcTable = schema.getSrcSchema().get(tableId);
    Map<String, SourceColumnDefinition> srcCols = srcTable.getColDefs();
    for (String colId : srcTable.getColIds()) {
      // If spanner columns do not contain this column Id, drop from change event.
      if (!spCols.containsKey(colId)) {
        ((ObjectNode) changeEvent).remove(srcCols.get(colId).getName());
      }
    }
    return changeEvent;
  }

  private static void populateMutationBuilderWithEvent(
      Table table,
      Mutation.WriteBuilder builder,
      JsonNode changeEvent,
      List<String> columnNames,
      Set<String> keyColumnNames)
      throws ChangeEventConvertorException {
    Set<String> columnNamesAsSet = new HashSet<>(columnNames);
    if (!columnNamesAsSet.containsAll(keyColumnNames)) {
      throw new ChangeEventConvertorException(
          "Missing key columns from change event. " + keyColumnNames);
    }
    for (String colName : columnNames) {
      Type columnType = table.column(colName).type();
      Value columnValue = null;

      boolean requiredField = keyColumnNames.contains(colName);
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
          // TODO: verify/fix data handling for bytes type once standard output format for bulk
          // reader is
          // decided.
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
        default:
          throw new IllegalArgumentException(
              "Column name("
                  + colName
                  + ") has unsupported column type("
                  + columnType.getCode()
                  + ")");
      }
      builder.set(colName).to(columnValue);
    }
  }
}
