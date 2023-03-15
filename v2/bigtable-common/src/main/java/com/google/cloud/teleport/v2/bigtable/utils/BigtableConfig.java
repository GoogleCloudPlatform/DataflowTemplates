/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.bigtable.utils;

import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions;

public class BigtableConfig {
  private BigtableConfig() {}

  public static CloudBigtableTableConfiguration generateCloudBigtableWriteConfiguration(
      BigtableCommonOptions.WriteOptions options) {
    String projectId = options.getBigtableWriteProjectId();
    CloudBigtableTableConfiguration.Builder builderBigtableTableConfig =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId(projectId == null ? options.getProject() : projectId)
            .withInstanceId(options.getBigtableWriteInstanceId())
            .withTableId(options.getBigtableWriteTableId())
            .withAppProfileId(options.getBigtableWriteAppProfile())
            .withConfiguration(BigtableOptionsFactory.INITIAL_ELAPSED_BACKOFF_MILLIS_KEY, "100")
            .withConfiguration(BigtableOptionsFactory.MAX_ELAPSED_BACKOFF_MILLIS_KEY, "600000");

    if (options.getBigtableBulkWriteLatencyTargetMs() != null) {
      builderBigtableTableConfig
          .withConfiguration(
              BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING, "true")
          .withConfiguration(
              BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS,
              String.valueOf(options.getBigtableBulkWriteLatencyTargetMs()));
    }
    if (options.getBigtableBulkWriteMaxRowKeyCount() != null) {
      builderBigtableTableConfig.withConfiguration(
          BigtableOptionsFactory.BIGTABLE_BULK_MAX_ROW_KEY_COUNT,
          String.valueOf(options.getBigtableBulkWriteMaxRowKeyCount()));
    }
    if (options.getBigtableBulkWriteMaxRequestSizeBytes() != null) {
      builderBigtableTableConfig.withConfiguration(
          BigtableOptionsFactory.BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES,
          String.valueOf(options.getBigtableBulkWriteMaxRequestSizeBytes()));
    }
    if (options.getBigtableRpcTimeoutMs() != null) {
      builderBigtableTableConfig.withConfiguration(
          BigtableOptionsFactory.BIGTABLE_RPC_TIMEOUT_MS_KEY,
          String.valueOf(options.getBigtableRpcTimeoutMs()));
    }
    if (options.getBigtableRpcAttemptTimeoutMs() != null) {
      builderBigtableTableConfig.withConfiguration(
          BigtableOptionsFactory.BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS_KEY,
          String.valueOf(options.getBigtableRpcAttemptTimeoutMs()));
    }

    if (options.getBigtableAdditionalRetryCodes() != null) {
      builderBigtableTableConfig.withConfiguration(
          BigtableOptionsFactory.ADDITIONAL_RETRY_CODES, options.getBigtableAdditionalRetryCodes());
    }

    return builderBigtableTableConfig.build();
  }
}
