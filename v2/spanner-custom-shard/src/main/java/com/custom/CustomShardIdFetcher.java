/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a custom sharding function for reverse migration.
 *
 * <p>This logic shards primarily based on the 'ddrkey' column.
 * It also includes specific test-case logic for certain tables.
 *
 * <p>The hash value is distributed across four logical shards:
 * 'shard1_00', 'shard1_01', 'shard2_00', 'shard2_01'.
 */
public class CustomShardIdFetcher implements IShardIdFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(CustomShardIdFetcher.class);
    private static final String DEFAULT_SHARD = "shard1_00";

    @Override
    public void init(String parameters) {
        LOG.info("CustomShardIdFetcher init called with {}", parameters);
    }

    /**
     * Calculates the logical shard ID for a given Spanner record.
     *
     * @param shardIdRequest The request containing the table name and Spanner record map.
     * @return A response object containing the calculated logical shard ID.
     */
    @Override
    public ShardIdResponse getShardId(ShardIdRequest shardIdRequest) {
        ShardIdResponse shardIdResponse = new ShardIdResponse();
        Map<String, Object> record = shardIdRequest.getSpannerRecord();
        String tableName = shardIdRequest.getTableName();
        Object shardingKeyObject = null;

        // Use 'ddrkey' if it exists.
        if (record.containsKey("ddrkey")) {
            shardingKeyObject = record.get("ddrkey");
        } else {
            // 3. Default Case (Edit 1: db_version/spanner_mutations_count fall here)
            // For any tables without 'ddrkey', assign to a default shard.
//            LOG.warn("Table {} does not contain 'ddrkey'. "
//                    + "Assigning to default shard '{}'.", tableName, DEFAULT_SHARD);
            shardIdResponse.setLogicalShardId(DEFAULT_SHARD);
            return shardIdResponse;
        }

        // Handle cases where the sharding key column exists but its value is null.
        String logicalShardId = getLogicalShardId(shardingKeyObject);
        shardIdResponse.setLogicalShardId(logicalShardId);

        LOG.debug("Assigned table {} to shard {} (Key={})",
                tableName, logicalShardId, shardingKeyObject);

        return shardIdResponse;
    }

    private String getLogicalShardId(Object shardingKeyObject) {
        // Handle cases where the sharding key column exists but its value is null.
        if (shardingKeyObject == null) {
            return DEFAULT_SHARD;
        }

        // 4. Get a numeric hash value from the key object.
        long hashValue = getHashValue(shardingKeyObject);

        // 5. Apply 4-bucket sharding logic.
        long bucket = Math.abs(hashValue % 4);
        String logicalShardId;

        if (bucket == 0) {
            logicalShardId = "shard1_00";
        } else if (bucket == 1) {
            logicalShardId = "shard1_01";
        } else if (bucket == 2) {
            logicalShardId = "shard2_00";
        } else { // bucket == 3
            logicalShardId = "shard2_01";
        }
        return logicalShardId;
    }
    /**
     * Safely converts a sharding key object into a numeric long for hashing.
     *
     * <p>This handles three cases:
     * 1. The object is already a Long.
     * 2. The object is a String representation of a Long (e.g., Spanner INT64 in reverse migration).
     * 3. The object is a non-numeric String (e.g., 'sql_hash'), in which case its hashCode is used.
     *
     * @param keyObject The sharding key (e.g., a Long or a String).
     * @return A long value to be used for hashing.
     */
    private long getHashValue(Object keyObject) {
        if (keyObject == null) {
            return 0L;
        }

        // Case 1: Already a Long
        if (keyObject instanceof Long) {
            return (Long) keyObject;
        }

        // Case 2: A String. Try to parse as Long first.
        if (keyObject instanceof String) {
            String s = (String) keyObject;
            try {
                // This handles INT64 (BIGINT) which comes as a String from Spanner
                return Long.parseLong(s);
            } catch (NumberFormatException e) {
                // This handles true strings.
                return (long) s.hashCode();
            }
        }

        // Case 3: Other number types (Integer, Double, BigDecimal, etc.)
        if (keyObject instanceof Number) {
            return ((Number) keyObject).longValue();
        }

        // Fallback for other types (byte arrays, etc.)
        return (long) keyObject.toString().hashCode();
    }
}