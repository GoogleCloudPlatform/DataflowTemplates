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
package com.custom;

import com.google.cloud.teleport.v2.spanner.utils.IShardIdFetcher;
import com.google.cloud.teleport.v2.spanner.utils.ShardIdRequest;
import com.google.cloud.teleport.v2.spanner.utils.ShardIdResponse;
import java.util.Map;

/**
 * A custom shard ID fetcher implementation used in sharded integration tests.
 *
 * <p>This fetcher evaluates the Spanner record's primary keys (either {@code CustomerId} or {@code
 * id}) and applies a modulo operation to dynamically route records to specific logical shards
 * ({@code testShardA} for odd numbers, {@code testShardB} for even numbers).
 *
 * <p>It is specifically designed for integration testing (e.g., verifying {@code retryAllDLQ}
 * behavior in a sharded schema) to ensure that the pipeline correctly extracts IDs and routes rows
 * to their target shards when native {@code migration_shard_id} schema columns are omitted.
 */
public class CustomShardIdFetcherForRetryIT implements IShardIdFetcher {

  @Override
  public void init(String parameters) {}

  @Override
  public ShardIdResponse getShardId(ShardIdRequest shardIdRequest) {
    Map<String, Object> keys = shardIdRequest.getSpannerRecord();

    // Use the Primary Key to identify the correct logical shard
    if (keys != null) {
      if (keys.containsKey("CustomerId")) {
        long customerId = Long.parseLong(keys.get("CustomerId").toString());
        long shardIdx = customerId % 2;

        ShardIdResponse response = new ShardIdResponse();
        if (shardIdx == 0) {
          response.setLogicalShardId("testShardB");
        } else {
          response.setLogicalShardId("testShardA");
        }
        return response;
      } else if (keys.containsKey("id")) {
        // Handle AllDataTypes which uses 'id' instead of 'CustomerId'
        long id = Long.parseLong(keys.get("id").toString());
        long shardIdx = id % 2;

        ShardIdResponse response = new ShardIdResponse();
        if (shardIdx == 0) {
          response.setLogicalShardId("testShardB");
        } else {
          response.setLogicalShardId("testShardA");
        }
        return response;
      }
    }

    return new ShardIdResponse();
  }
}
