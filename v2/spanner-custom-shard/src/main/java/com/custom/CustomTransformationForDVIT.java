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

import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomTransformationForDVIT implements ISpannerMigrationTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(CustomTransformationForDVIT.class);
  private String customParam = "";

  @Override
  public void init(String parameters) {
    LOG.info("init called with {}", parameters);
    if (parameters != null) {
      this.customParam = parameters;
    }
  }

  @Override
  public MigrationTransformationResponse toSpannerRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {

    Map<String, Object> row = request.getRequestRow();
    Map<String, Object> responseRow = new HashMap<>();

    if ("Users".equals(request.getTableName())) {
      // Filter out records where age is 99
      if (row.get("age") != null && ((Number) row.get("age")).intValue() == 99) {
        return new MigrationTransformationResponse(new HashMap<>(), true);
      }

      // Convert full_name to a hex string representation
      if (row.get("full_name") != null) {
        String name = row.get("full_name").toString();
        String val = name + customParam;
        responseRow.put(
            "full_name", HexFormat.of().formatHex(val.getBytes(StandardCharsets.UTF_8)));
      }

      // Parse created_at and add 1 hour as a data transformation
      if (row.get("created_at") != null) {
        Instant t = Instant.parse((String) row.get("created_at"));
        Instant shifted = t.plus(1, ChronoUnit.HOURS);
        responseRow.put("created_at", shifted);
      }

      return new MigrationTransformationResponse(responseRow, false);
    }

    if ("Users_PKTransformed".equals(request.getTableName())) {
      // Transform Primary Key user_id
      if (row.get("user_id") != null) {
        long id = ((Number) row.get("user_id")).longValue();
        responseRow.put("user_id", id + 10);
      }

      return new MigrationTransformationResponse(responseRow, false);
    }

    if ("Users_AddedColumn".equals(request.getTableName())) {
      responseRow.put("status", "ACTIVE");
      return new MigrationTransformationResponse(responseRow, false);
    }
    return new MigrationTransformationResponse(null, false);
  }

  @Override
  public MigrationTransformationResponse toSourceRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    return new MigrationTransformationResponse(null, false);
  }

  @Override
  public MigrationTransformationResponse transformFailedSpannerMutation(
      MigrationTransformationRequest request) throws InvalidTransformationException {
    return new MigrationTransformationResponse(null, false);
  }
}
