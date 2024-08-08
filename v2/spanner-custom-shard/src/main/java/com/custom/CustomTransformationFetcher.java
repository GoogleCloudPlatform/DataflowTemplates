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
 * This is a sample class to be implemented by the customer. All the relevant dependencies have been
 * added and users need to implement the toSpannerRow() and/or toSourceRow() method for forward and
 * reverse replication flows respectively
 */
public class CustomTransformationFetcher implements ISpannerMigrationTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(CustomShardIdFetcher.class);

  @Override
  public void init(String customParameters) {
    LOG.info("init called with {}", customParameters);
  }

  @Override
  public MigrationTransformationResponse toSpannerRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    if (request.getTableName().equals("Customers")) {
      Map<String, Object> requestRow = request.getRequestRow();
      Map<String, Object> responseRow = new HashMap<>();

      responseRow.put(
          "full_name", requestRow.get("first_name") + " " + requestRow.get("last_name"));
      responseRow.put("migration_shard_id", request.getShardId() + "_" + requestRow.get("id"));
      MigrationTransformationResponse response =
          new MigrationTransformationResponse(responseRow, false);
      return response;
    }
    return new MigrationTransformationResponse(null, false);
  }

  @Override
  public MigrationTransformationResponse toSourceRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    if (request.getTableName().equals("Customers")) {
      Map<String, Object> requestRow = request.getRequestRow();
      Map<String, Object> responseRow = new HashMap<>();
      String fullName = (String) requestRow.get("full_name");
      String[] nameParts = fullName.split(" ", 2);
      responseRow.put("first_name", nameParts[0]);
      responseRow.put("last_name", nameParts[1]);
      String migrationShardId = (String) requestRow.get("migration_shard_id");
      String[] idParts = migrationShardId.split("_", 2);
      responseRow.put("id", idParts[1]);
      MigrationTransformationResponse response =
          new MigrationTransformationResponse(responseRow, false);
      return response;
    }
    return new MigrationTransformationResponse(null, false);
  }
}
