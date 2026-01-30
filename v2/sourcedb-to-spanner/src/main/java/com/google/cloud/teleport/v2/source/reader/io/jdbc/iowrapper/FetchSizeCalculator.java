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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculates the fetch size for JDBC readers based on worker resources and row size estimation.
 * Formula: FetchSize = (WorkerMemory) / (4 * WorkerCores * MaxRowSize)
 */
public final class FetchSizeCalculator {
  private static final Logger LOG = LoggerFactory.getLogger(FetchSizeCalculator.class);

  private static final int MIN_FETCH_SIZE = 1;
  private static final int MAX_FETCH_SIZE = Integer.MAX_VALUE;

  private FetchSizeCalculator() {}

  /**
   * @param estimatedRowSize Estimated size of a row in bytes.
   * @param workerMemoryGB The Dataflow worker memory in GB.
   * @param workerCores The Dataflow worker cores.
   * @return The calculated fetch size, or 0 if it cannot be calculated.
   */
  public static Integer getFetchSize(
      TableConfig tableConfig, long estimatedRowSize, Double workerMemoryGB, Integer workerCores) {
    if (tableConfig.fetchSize() != null) {
      LOG.info(
          "Explicitly configured fetch size for table {}: {}",
          tableConfig.tableName(),
          tableConfig.fetchSize());
      return tableConfig.fetchSize();
    }

    try {
      if (estimatedRowSize == 0) {
        LOG.warn(
            "Estimated row size is 0 for table {}. FetchSize cannot be calculated. Cursor mode will not be enabled.",
            tableConfig.tableName());
        return 0;
      }

      if (workerMemoryGB == null || workerCores == null) {
        LOG.warn(
            "Worker memory or cores unavailable. FetchSize cannot be calculated. Cursor mode will not be enabled.");
        return 0;
      }

      long workerMemoryBytes = (long) (workerMemoryGB * 1024 * 1024 * 1024);

      // Formula: (Memory of Dataflow worker VM) / (2 * 2 * (Number of cores on the
      // Dataflow worker VM) * (Maximum row size))
      // 2 * 2 = 4 (Safety factor)
      long denominator = 4L * workerCores * estimatedRowSize;

      if (denominator == 0) { // Should not happen given estimatedRowSize check and cores >= 1
        LOG.warn(
            "Denominator for fetch size calculation is zero for table {}. FetchSize cannot be calculated. Cursor mode will not be enabled.",
            tableConfig.tableName());
        return 0;
      }

      long calculatedFetchSize = workerMemoryBytes / denominator;

      LOG.info(
          "Auto-inferred fetch size for table {}: {} (Memory: {} bytes, Cores: {}, RowSize: {} bytes)",
          tableConfig.tableName(),
          calculatedFetchSize,
          workerMemoryBytes,
          workerCores,
          estimatedRowSize);

      if (calculatedFetchSize < MIN_FETCH_SIZE) {
        return MIN_FETCH_SIZE;
      }
      if (calculatedFetchSize > MAX_FETCH_SIZE) {
        return MAX_FETCH_SIZE;
      }

      return (int) calculatedFetchSize;

    } catch (Exception e) {
      LOG.warn(
          "Failed to auto-infer fetch size for table {}, error: {}. Cursor mode will not be enabled.",
          tableConfig.tableName(),
          e.getMessage());
      return 0;
    }
  }
}
