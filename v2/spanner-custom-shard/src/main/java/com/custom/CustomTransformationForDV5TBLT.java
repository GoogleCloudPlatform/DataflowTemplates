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
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom transformation used by the {@code GCSSpannerDV5TBLT} load test of the gcs-spanner-dv
 * template.
 *
 * <p>It mutates {@code col1} of every source record in {@code table1} and {@code table2} unless the
 * record belongs to {@code shard_1}. Because the validator hashes the transformed source record,
 * every record outside {@code shard_1} stops matching its Spanner row. The test therefore validates
 * one fully matching shard and pushes a large volume of mismatches through the report stage.
 */
public class CustomTransformationForDV5TBLT implements ISpannerMigrationTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(CustomTransformationForDV5TBLT.class);

  static final String MATCHING_SHARD_ID = "shard_1";
  static final Set<String> MUTATED_TABLES = Set.of("table1", "table2");
  static final String MUTATED_COLUMN = "col1";
  static final String MUTATION_SUFFIX = "_mutated";

  // Logged once per transformer instance so that workers confirm the transformation is active
  // without emitting one log line per record (the test processes hundreds of millions of rows).
  private transient boolean loggedFirstMutation = false;

  @Override
  public void init(String parameters) {
    LOG.info(
        "[5TB-LT] CustomTransformationForDV5TBLT initialized. parameters={} (ignored),"
            + " matchingShard={}, mutatedTables={}, mutatedColumn={}, suffix={}",
        parameters,
        MATCHING_SHARD_ID,
        MUTATED_TABLES,
        MUTATED_COLUMN,
        MUTATION_SUFFIX);
  }

  @Override
  public MigrationTransformationResponse toSpannerRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    String tableName = request.getTableName();
    String shardId = request.getShardId();
    if (MATCHING_SHARD_ID.equals(shardId) || !MUTATED_TABLES.contains(tableName)) {
      return new MigrationTransformationResponse(null, false);
    }
    Map<String, Object> row = request.getRequestRow();
    Object value = row == null ? null : row.get(MUTATED_COLUMN);
    if (value == null) {
      return new MigrationTransformationResponse(null, false);
    }
    String mutatedValue = value + MUTATION_SUFFIX;
    if (!loggedFirstMutation) {
      loggedFirstMutation = true;
      LOG.info(
          "[5TB-LT] First mutation on this worker: table={}, shardId={}, originalLength={},"
              + " mutatedLength={}",
          tableName,
          shardId,
          value.toString().length(),
          mutatedValue.length());
    }
    return new MigrationTransformationResponse(Map.of(MUTATED_COLUMN, mutatedValue), false);
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
