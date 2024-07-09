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

public class CustomTransformation implements ISpannerMigrationTransformer {

  @Override
  public void init(String customParameters) {}

  @Override
  public MigrationTransformationResponse toSpannerRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    if (request.getTableName().equals("Singers")) {
      Map<String, Object> row = new HashMap<>(request.getRequestRow());
      Long singerId = (Long) row.get("SingerId");
      if (singerId > 50) {
        return new MigrationTransformationResponse(request.getRequestRow(), true);
      }
      row.put("SingerId", singerId + 100);
      row.put("FirstName", (String) row.get("FirstName") + (String) row.get("migration_shard_id"));
      return new MigrationTransformationResponse(row, false);
    }
    return new MigrationTransformationResponse(request.getRequestRow(), false);
  }

  @Override
  public MigrationTransformationResponse toSourceRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    return null;
  }
}
