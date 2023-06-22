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

import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import com.google.cloud.teleport.v2.templates.common.TrimmedDataChangeRecord;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This DoFn assigns the shardId as key to the record. */
public class AssignShardIdFn
    extends DoFn<TrimmedDataChangeRecord, KV<String, TrimmedDataChangeRecord>> {
  private static final Logger LOG = LoggerFactory.getLogger(AssignShardIdFn.class);

  private final Schema schema;

  public AssignShardIdFn(Schema schema) {
    LOG.info("Found schema in constructor: " + schema);
    this.schema = schema;
    LOG.info("Found schema in constructor2: " + this.schema);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    TrimmedDataChangeRecord record = c.element();

    try {
      String shardIdColumn = getShardIdColumnForTableName(record.getTableName());
      String keysJsonStr = record.getMods().get(0).getKeysJson();
      String newValueJsonStr = record.getMods().get(0).getNewValuesJson();
      JSONObject keysJson = new JSONObject(keysJsonStr);
      JSONObject newValueJson = new JSONObject(newValueJsonStr);
      if (newValueJson.has(shardIdColumn)) {
        String shardId = newValueJson.getString(shardIdColumn);
        c.output(KV.of(shardId, record));
      } else if (keysJson.has(shardIdColumn)) {
        String shardId = keysJson.getString(shardIdColumn);
        c.output(KV.of(shardId, record));
      } else {
        LOG.error(
            "Cannot find entry for HarbourBridge shard id column '"
                + shardIdColumn
                + "' in record.");
        return;
      }
    } catch (IllegalArgumentException e) {
      LOG.error("Error fetching shard Id column for table: " + e.getMessage());
      return;
    } catch (Exception e) {
      StringWriter errors = new StringWriter();
      e.printStackTrace(new PrintWriter(errors));
      LOG.error("Error while parsing json: " + e.getMessage() + ": " + errors.toString());
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
}
