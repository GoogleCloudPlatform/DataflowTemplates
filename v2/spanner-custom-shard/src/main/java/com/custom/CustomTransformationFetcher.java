/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.custom;

import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.common.hash.Hashing;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom transformation to generate a 'ddrkey' for hotspot prevention.
 */
public class CustomTransformationFetcher implements ISpannerMigrationTransformer, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(CustomTransformationFetcher.class);
    private static final String TARGET_TABLE1 = "ut_academy_tradeable_player";
    private static final String TARGET_TABLE2 = "utas_userbased_settings";
    @Override
    public void init(String customParameters) {
        LOG.info("CustomTransformationFetcher init called with: {}", customParameters);
    }

    @Override
    public MigrationTransformationResponse toSpannerRow(MigrationTransformationRequest request)
            throws InvalidTransformationException {

        // --- 1. Check if this row is from our target table ---
        if (request.getTableName().equals(TARGET_TABLE1)) {

            Map<String, Object> requestRow = request.getRequestRow();
            Map<String, Object> responseRow = new HashMap<>();

            try {
                // --- 2. Get the original key parts ---
                // Your schema shows BIGINT, which JDBC maps to Long
                Long ddrkey = (Long) requestRow.get("ddrkey");
                Long userId = (Long) requestRow.get("userId");
                Long itemId = (Long) requestRow.get("itemId");

                // Simple null check (PK columns should not be null, but good to check)
                if (ddrkey == null || userId == null || itemId == null) {
                    throw new InvalidTransformationException("PK column is null for table " + TARGET_TABLE1);
                }

                // --- 3. Create the deterministic key string ---
                String keyString = ddrkey.toString() + ":" + userId.toString() + ":" + itemId.toString();

                // --- 4. Hash the string using Murmur3 to get a 64-bit LONG (INT64) ---
                long uuidLong =
                        Hashing.murmur3_128().hashString(keyString, StandardCharsets.UTF_8).asLong();

                String uuid = Long.toString(uuidLong);
                // --- 5. Add the new ddrkey to the row ---
                // This is the only column we are adding/modifying.
                responseRow.put("uuid", uuid);
                LOG.info("uuid: {}", uuid);
                LOG.info("responseRow with uuid : {}", responseRow);
                // Return the modified row. 'false' means "do not filter this row".
                return new MigrationTransformationResponse(responseRow, false);

            } catch (Exception e) {
                LOG.error("Error generating uuid for table {}: {}", TARGET_TABLE1, e.getMessage());
                // Propagate the error to fail the row
                throw new InvalidTransformationException("Failed to generate uuid: " + e.getMessage());
            }
        }

        else if (request.getTableName().equals(TARGET_TABLE2)) {

            Map<String, Object> requestRow = request.getRequestRow();
            Map<String, Object> responseRow = new HashMap<>();

            try {
                // --- 2. Get the original key parts ---
                // Your schema shows BIGINT, which JDBC maps to Long
                Long nucUserId = (Long) requestRow.get("nucUserId");
                Long nucPersId = (Long) requestRow.get("nucPersId");
                Long gameSku = (Long) requestRow.get("gameSku");

                // Simple null check (PK columns should not be null, but good to check)
                if (nucUserId == null || nucPersId == null || gameSku == null) {
                    throw new InvalidTransformationException("PK column is null for table " + TARGET_TABLE1);
                }

                // --- 3. Create the deterministic key string ---
                String keyString = nucUserId.toString() + ":" + nucPersId.toString() + ":" + gameSku.toString();

                // --- 4. Hash the string using Murmur3 to get a 64-bit LONG (INT64) ---
                long publicVal =
                        Hashing.murmur3_128().hashString(keyString, StandardCharsets.UTF_8).asLong();

                // --- 5. Add the new ddrkey to the row ---
                // This is the only column we are adding/modifying.
                responseRow.put("public", publicVal);
                LOG.info("public: {}", publicVal);
                LOG.info("responseRow with public : {}", responseRow);
                // Return the modified row. 'false' means "do not filter this row".
                return new MigrationTransformationResponse(responseRow, false);

            } catch (Exception e) {
                LOG.error("Error generating public_val for table {}: {}", TARGET_TABLE2, e.getMessage());
                // Propagate the error to fail the row
                throw new InvalidTransformationException("Failed to generate public_val: " + e.getMessage());
            }
        }

        // For all other tables, return an empty map (no changes, do not filter)
        return new MigrationTransformationResponse(new HashMap<>(), false);
    }

    @Override
    public MigrationTransformationResponse toSourceRow(MigrationTransformationRequest request)
            throws InvalidTransformationException {

        if (request.getTableName().contains("unified_lgm_event_data")) {
            LOG.info("CustomTransformationFetcher toSourceRow for table unified_lgm_event_data called with: {}", request);
            Map<String, Object> requestRow = request.getRequestRow();
            Map<String, Object> responseRow = new HashMap<>();

            try {
                String ddrkey = (String) requestRow.get("ddrkey");
                String userId = (String) requestRow.get("userId");
                String slotId = (String) requestRow.get("slotId");

                if (ddrkey == null || userId == null || slotId == null) {
                    throw new InvalidTransformationException("PK column is null for table unified_lgm_event_data");
                }

                String keyString = ddrkey + ":" + userId + ":" + slotId;
                Long lgmMode =
                        Hashing.murmur3_128().hashString(keyString, StandardCharsets.UTF_8).asLong();
                responseRow.put("lgmMode", lgmMode.toString());

                LOG.info("lgmMode: {}", lgmMode);
                LOG.info("responseRow with lgmMode : {}", responseRow);

                return new MigrationTransformationResponse(responseRow, false);

            } catch (Exception e) {
                LOG.error("Error generating lgmMode for table unified_lgm_event_data: {}", e.getMessage());
                throw new InvalidTransformationException("Failed to generate lgmMode: " + e.getMessage());
            }
        }

        if (request.getTableName().contains("fcas_user_matches")) {
            LOG.info("CustomTransformationFetcher toSourceRow for table fcas_user_matches called with: {}", request);
            Map<String, Object> requestRow = request.getRequestRow();
            Map<String, Object> responseRow = new HashMap<>();

            try {
                String ddrkey = (String) requestRow.get("ddrkey");
                String userId = (String) requestRow.get("userId");
                String matchId = (String) requestRow.get("matchId");

                if (ddrkey == null || userId == null || matchId == null) {
                    throw new InvalidTransformationException("PK column is null for table unified_lgm_event_data");
                }

                String keyString = ddrkey + ":" + userId + ":" + matchId;
                Long round =
                        Hashing.murmur3_128().hashString(keyString, StandardCharsets.UTF_8).asLong();
                responseRow.put("round", round.toString());

                LOG.info("round: {}", round);
                LOG.info("responseRow with round : {}", responseRow);

                return new MigrationTransformationResponse(responseRow, false);

            } catch (Exception e) {
                LOG.error("Error generating round for table fcas_user_matches: {}", e.getMessage());
                throw new InvalidTransformationException("Failed to generate round: " + e.getMessage());
            }
        }

        return new MigrationTransformationResponse(new HashMap<>(), false);
    }
}
