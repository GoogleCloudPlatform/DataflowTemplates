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

public class CustomTransformationFetcher implements ISpannerMigrationTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(CustomTransformationFetcher.class);

  @Override
  public void init(String customParameters) {
    LOG.info("init called with {}", customParameters);
  }

  @Override
  public MigrationTransformationResponse toSpannerRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {

    String tableName = request.getTableName();

    // Flow 1: Purposefully return an error to test standard retry path
    if (tableName.equalsIgnoreCase("table1_transform_fail")) {
      Map<String, Object> requestRow = request.getRequestRow();
      String trigger = (String) requestRow.get("trigger_fail");
      if ("FAIL".equals(trigger)) {
        throw new InvalidTransformationException("Simulated Transformation Error for Flow 1");
      }
    }

    // Flows 2, 3, 4, 5: Return NULL/Empty.
    // This explicitly tells Dataflow to use the Session File for mapping.
    // It will automatically map 'src_name' -> 'spanner_name' correctly.
    return new MigrationTransformationResponse(new HashMap<>(), false);
  }

  @Override
  public MigrationTransformationResponse toSourceRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    return new MigrationTransformationResponse(new HashMap<>(), false);
  }

  @Override
  public MigrationTransformationResponse transformFailedSpannerMutation(
      MigrationTransformationRequest request) throws InvalidTransformationException {

    Map<String, Object> requestRow = request.getRequestRow();
    Map<String, Object> responseRow = new HashMap<>();
    String tableName = request.getTableName();

    LOG.info("Processing failed mutation for table: " + tableName);

    // Flow 4: Fix constraint violation by truncating string
    // Because this input comes from a Spanner Write failure, the keys are already Spanner Column
    // Names.
    if (tableName.equalsIgnoreCase("table3_constraint")) {
      String colName = "spanner_data_col";
      if (requestRow.containsKey(colName)) {
        String originalValue = (String) requestRow.get(colName);
        if (originalValue != null && originalValue.length() > 10) {
          String newValue = originalValue.substring(0, 10);
          responseRow.put(colName, newValue);
          LOG.info("Flow 4 Fix: Truncated {} from '{}' to '{}'", colName, originalValue, newValue);
          return new MigrationTransformationResponse(responseRow, false);
        }
      }
    }

    // Return the (potentially modified) row to be retried
    return new MigrationTransformationResponse(new HashMap<>(), false);
  }
}
