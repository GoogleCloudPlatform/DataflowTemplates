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
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom transformation class used for DLQ testing in Spanner to Source reverse replication.
 *
 * <p>Overview: This transformer injects severe transformation errors to test the pipeline's Dead
 * Letter Queue (DLQ) processing and error handling.
 *
 * <p>Test Usage modes: - mode=bad: Intentionally throws InvalidTransformationException on rows with
 * ID 999 and 888 in the AllDataTypes table. Used during the main pipeline run to force these rows
 * into the severe DLQ error bucket. - mode=semi-fixed: Intentionally throws an exception only on
 * row ID 888. This simulates a scenario where a user pushes a corrected transformation JAR for the
 * retry pipeline. Row 999 successfully migrates during the retry, while row 888 correctly fails
 * again and gets routed back into the DLQ.
 *
 * <p>For the Orders table, it translates the OrderSource column into a custom LegacyOrderSystem
 * column to validate standard data transformation operations.
 */
public class SpannerToSourceDbRetryTransformation implements ISpannerMigrationTransformer {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToSourceDbRetryTransformation.class);

  private String mode = "bad";

  @Override
  public void init(String parameters) {
    LOG.info("init called with {}", parameters);
    if (parameters != null) {
      String[] parts = parameters.split(",");
      for (String part : parts) {
        String[] kv = part.split("=");
        if (kv.length == 2) {
          if (kv[0].trim().equals("mode")) {
            this.mode = kv[1].trim();
          }
        }
      }
    }
    LOG.info("Mode set to {}", this.mode);
  }

  @Override
  public MigrationTransformationResponse toSpannerRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    return new MigrationTransformationResponse(new HashMap<>(), false);
  }

  @Override
  public MigrationTransformationResponse toSourceRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    String tableName = request.getTableName();
    Map<String, Object> requestRow = request.getRequestRow();
    Map<String, Object> responseRow = new HashMap<>();

    LOG.info("Processing table {} in mode {}", tableName, mode);

    if (tableName.equalsIgnoreCase("Orders")) {
      Object sourceObj = requestRow.get("OrderSource");
      String source = (sourceObj != null) ? (String) sourceObj : "UNKNOWN_SYSTEM";
      // Enclose in single quotes since DML generator injects custom responses directly as raw
      // values
      String legacySys = "'" + source + "_v1'";
      responseRow.put("LegacyOrderSystem", legacySys);
      return new MigrationTransformationResponse(responseRow, false);
    } else if (tableName.equalsIgnoreCase("AllDataTypes")) {
      Object idObj = requestRow.get("id");
      Long id = null;
      if (idObj instanceof Number) {
        id = ((Number) idObj).longValue();
      } else if (idObj instanceof String) {
        id = Long.parseLong((String) idObj);
      }

      if (id != null) {
        if (mode.equalsIgnoreCase("bad")) {
          if (id == 999 || id == 888) {
            LOG.info("Crashing on id {} for table {} in mode {}", id, tableName, mode);
            throw new InvalidTransformationException("Simulated failure for id " + id);
          }
        } else if (mode.equalsIgnoreCase("semi-fixed")) {
          if (id == 888) {
            LOG.info("Crashing on id {} for table {} in mode {}", id, tableName, mode);
            throw new InvalidTransformationException("Simulated failure for id " + id);
          }
        }
      }
      return new MigrationTransformationResponse(responseRow, false);
    }

    return new MigrationTransformationResponse(responseRow, false);
  }

  @Override
  public MigrationTransformationResponse transformFailedSpannerMutation(
      MigrationTransformationRequest request) throws InvalidTransformationException {
    return new MigrationTransformationResponse(new HashMap<>(), false);
  }
}
