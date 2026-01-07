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
package com.google.cloud.teleport.v2.templates.utils;

import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.utils.IShardIdFetcher;
import com.google.cloud.teleport.v2.spanner.utils.ShardIdRequest;
import com.google.cloud.teleport.v2.spanner.utils.ShardIdResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default implementation to get the shard identifier. */
public class ShardIdFetcherImpl implements IShardIdFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(ShardIdFetcherImpl.class);
  private final ISchemaMapper schemaMapper;
  private final String skipDirName;

  public ShardIdFetcherImpl(ISchemaMapper schemaMapper, String skipDirName) {
    this.schemaMapper = schemaMapper;
    this.skipDirName = skipDirName;
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
        LOG.error(
            "Cannot find entry for the shard id column '"
                + shardIdColumn
                + "' in record."); // aastha error - caught by AssignShardIdFn and classified correctly
        throw new RuntimeException(
            "Cannot find entry for the shard id column '" + shardIdColumn + "' in record.");
      }
    } catch (IllegalArgumentException e) {
      LOG.error("Error fetching shard Id column for table: " + e.getMessage()); // aastha error - caught by AssignShardIdFn and classified correctly
      throw new RuntimeException("Error fetching shard Id column for table: " + e.getMessage());
    } catch (Exception e) {
      StringWriter errors = new StringWriter();
      e.printStackTrace(new PrintWriter(errors));
      LOG.error(
          "Error fetching shard Id colum: "
              + e.getMessage()
              + ": "
              + errors.toString()); // aastha error - caught by AssignShardIdFn and classified correctly
      throw new RuntimeException(
          "Error fetching shard Id colum: " + e.getMessage() + ": " + errors.toString());
    }
  }

  private String getShardIdColumnForTableName(String tableName) throws IllegalArgumentException {
    try {
      String shardIdColumn = schemaMapper.getShardIdColumnName("", tableName);
      if (shardIdColumn == null || shardIdColumn.isEmpty()) {
        LOG.warn(
            "Table {} found in change record but not found in session file. Skipping record",
            tableName); // aastha warning - causes above function to error out, which then gets caught by AssignShardIdFn
        return "";
      }
      return shardIdColumn;
    } catch (NoSuchElementException e) {
      LOG.warn("Table {} not found in session file. Skipping record.", tableName); // aastha warning - causes above function to error out, which then gets caught by AssignShardIdFn
      return "";
    }
  }

  @Override
  public void init(String parameters) {
    LOG.info("init called with {}", parameters);
  }
}
