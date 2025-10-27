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
 * distributed under the License is distributed on an "AS IS IS" BASIS, WITHOUT
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
import java.util.UUID;
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

    // Check for the correct table name (case-insensitive is safer)
    if (request.getTableName().equalsIgnoreCase("ih_items_archive")) {
      Map<String, Object> responseRow = new HashMap<>();

      // Generate the random UUID for the new primary key column.
      responseRow.put("my_pk", UUID.randomUUID().toString());

      // Return the response.
      // Note: We only need to return the NEW or MODIFIED columns.
      // SMT will automatically carry over all other columns from the source.
      return new MigrationTransformationResponse(responseRow, false);
    }

    // For any other tables, return a response with an EMPTY, NON-NULL map.
    return new MigrationTransformationResponse(new HashMap<>(), false);
  }

  @Override
  public MigrationTransformationResponse toSourceRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    // This part is not used for your forward migration.
    // Return a default empty response.
    return new MigrationTransformationResponse(new HashMap<>(), false);
  }
}