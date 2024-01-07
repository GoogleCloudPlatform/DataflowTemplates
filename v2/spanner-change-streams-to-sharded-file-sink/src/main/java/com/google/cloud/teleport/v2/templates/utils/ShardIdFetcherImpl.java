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
package com.google.cloud.teleport.v2.templates.utils;

import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import com.google.cloud.teleport.v2.spanner.utils.IShardIdFetcher;
import com.google.cloud.teleport.v2.spanner.utils.ShardIdRequest;
import com.google.cloud.teleport.v2.spanner.utils.ShardIdResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default implementation to get the shard identifier. */
public class ShardIdFetcherImpl implements IShardIdFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(ShardIdFetcherImpl.class);
  private final Schema schema;

  public ShardIdFetcherImpl(Schema schema) {
    this.schema = schema;
  }

  @Override
  public ShardIdResponse getShardId(ShardIdRequest shardIdRequest) {
    try {
      String tableName = shardIdRequest.getTableName();
      String shardIdColumn = getShardIdColumnForTableName(tableName);
      if (shardIdRequest.getSpannerRecord().containsKey(shardIdColumn)) {
        String shardId = shardIdRequest.getSpannerRecord().get(shardIdColumn).toString();
        ShardIdResponse shardIdResponse = new ShardIdResponse();
        shardIdResponse.setLogicalShardId(shardId);
        return shardIdResponse;
      } else {
        LOG.error("Cannot find entry for the shard id column '" + shardIdColumn + "' in record.");
        throw new RuntimeException(
            "Cannot find entry for the shard id column '" + shardIdColumn + "' in record.");
      }
    } catch (IllegalArgumentException e) {
      LOG.error("Error fetching shard Id column for table: " + e.getMessage());
      throw new RuntimeException("Error fetching shard Id column for table: " + e.getMessage());
    } catch (Exception e) {
      StringWriter errors = new StringWriter();
      e.printStackTrace(new PrintWriter(errors));
      LOG.error("Error fetching shard Id colum: " + e.getMessage() + ": " + errors.toString());
      throw new RuntimeException(
          "Error fetching shard Id colum: " + e.getMessage() + ": " + errors.toString());
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
