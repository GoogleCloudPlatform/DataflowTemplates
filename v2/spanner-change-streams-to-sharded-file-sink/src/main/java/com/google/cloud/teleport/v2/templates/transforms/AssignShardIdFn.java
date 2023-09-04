/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.transforms;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.changestream.DataChangeRecordTypeConvertor;
import com.google.cloud.teleport.v2.templates.common.TrimmedShardedDataChangeRecord;
import com.google.common.collect.ImmutableList;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This DoFn assigns the shardId as key to the record. */
public class AssignShardIdFn
    extends DoFn<TrimmedShardedDataChangeRecord, TrimmedShardedDataChangeRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(AssignShardIdFn.class);

  private final SpannerConfig spannerConfig;

  /* SpannerAccessor must be transient so that its value is not serialized at runtime. */
  private transient SpannerAccessor spannerAccessor;

  /* The information schema of the Cloud Spanner database */
  private final Ddl ddl;

  private final Schema schema;

  // Jackson Object mapper.
  private transient ObjectMapper mapper;

  public AssignShardIdFn(SpannerConfig spannerConfig, Schema schema, Ddl ddl) {
    this.spannerConfig = spannerConfig;
    this.schema = schema;
    this.ddl = ddl;
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    if (spannerConfig != null) {
      spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    }
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
  }

  /** Teardown function disconnects from the Cloud Spanner. */
  @Teardown
  public void teardown() {
    if (spannerConfig != null) {
      spannerAccessor.close();
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    TrimmedShardedDataChangeRecord record = new TrimmedShardedDataChangeRecord(c.element());

    try {
      String shardIdColumn = getShardIdColumnForTableName(record.getTableName());

      String keysJsonStr = record.getMods().get(0).getKeysJson();
      JsonNode keysJson = mapper.readTree(keysJsonStr);

      String newValueJsonStr = record.getMods().get(0).getNewValuesJson();
      JsonNode newValueJson = mapper.readTree(newValueJsonStr);

      if (keysJson.has(shardIdColumn)) {
        String shardId = keysJson.get(shardIdColumn).asText();
        record.setShard(shardId);
        c.output(record);
      } else if (newValueJson.has(shardIdColumn)) {
        String shardId = newValueJson.get(shardIdColumn).asText();
        record.setShard(shardId);
        c.output(record);
      } else if (record.getModType() == ModType.DELETE) {
        String shardId =
            fetchShardId(
                record.getTableName(), record.getCommitTimestamp(), shardIdColumn, keysJson);
        record.setShard(shardId);
        c.output(record);
      } else {
        LOG.error("Cannot find entry for the shard id column '" + shardIdColumn + "' in record.");
        return;
      }
    } catch (IllegalArgumentException e) {
      LOG.error("Error fetching shard Id column for table: " + e.getMessage());
      return;
    } catch (Exception e) {
      StringWriter errors = new StringWriter();
      e.printStackTrace(new PrintWriter(errors));
      LOG.error("Error fetching shard Id colum: " + e.getMessage() + ": " + errors.toString());
      return;
    }
  }

  private String getShardIdColumnForTableName(String tableName) throws IllegalArgumentException {
    if (!schema.getSpannerToID().containsKey(tableName)) {
      throw new IllegalArgumentException(
          "Table " + tableName + " found in change record but not found in session file.");
    }
    String tableId = schema.getSpannerToID().get(tableName).getName();
    if (!schema.getSpSchema().containsKey(tableId)) {
      throw new IllegalArgumentException(
          "Table " + tableId + " not found in session file. Please provide a valid session file.");
    }
    SpannerTable spTable = schema.getSpSchema().get(tableId);
    String shardColId = spTable.getShardIdColumn();
    if (!spTable.getColDefs().containsKey(shardColId)) {
      throw new IllegalArgumentException(
          "ColumnId "
              + shardColId
              + " not found in session file. Please provide a valid session file.");
    }
    return spTable.getColDefs().get(shardColId).getName();
  }

  // Perform a stale read on spanner to fetch shardId.
  private String fetchShardId(
      String tableName,
      com.google.cloud.Timestamp commitTimestamp,
      String shardIdColName,
      JsonNode keysJson)
      throws Exception {
    com.google.cloud.Timestamp staleReadTs =
        com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
            commitTimestamp.getSeconds() - 1, commitTimestamp.getNanos());
    Struct row =
        spannerAccessor
            .getDatabaseClient()
            .singleUse(TimestampBound.ofReadTimestamp(staleReadTs))
            .readRow(tableName, generateKey(tableName, keysJson), Arrays.asList(shardIdColName));
    if (row == null) {
      throw new Exception("stale read on Spanner returned null");
    }
    return row.getString(0);
  }

  private com.google.cloud.spanner.Key generateKey(String tableName, JsonNode keysJson)
      throws Exception {
    try {
      Table table = ddl.table(tableName);
      ImmutableList<IndexColumn> keyColumns = table.primaryKeys();
      com.google.cloud.spanner.Key.Builder pk = com.google.cloud.spanner.Key.newBuilder();

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
          case JSON:
          case PG_JSONB:
            pk.append(
                DataChangeRecordTypeConvertor.toString(
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
}
