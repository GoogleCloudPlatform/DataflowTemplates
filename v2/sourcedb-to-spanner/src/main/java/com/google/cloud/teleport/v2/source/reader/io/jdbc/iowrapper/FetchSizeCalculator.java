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
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculates the fetch size for JDBC readers based on worker resources and row
 * size estimation.
 * Formula: FetchSize = (WorkerMemory) / (4 * WorkerCores * MaxRowSize)
 */
public final class FetchSizeCalculator {
  private static final Logger LOG = LoggerFactory.getLogger(FetchSizeCalculator.class);

  // Default fetch size if calculation fails or results in a value too
  // small/large.
  private static final int DEFAULT_FETCH_SIZE = 50_000;
  private static final int MIN_FETCH_SIZE = 100;
  // Cap at a reasonable max to avoid issues with some drivers.
  private static final int MAX_FETCH_SIZE = 100_000;

  private FetchSizeCalculator() {
  }

  /**
   * Calculates the fetch size for the given table.
   *
   * @param tableConfig      The table configuration.
   * @param estimatedRowSize Estimated size of a row in bytes.
   * @return The calculated fetch size.
   */
  @SuppressWarnings("null")
  public static int getFetchSize(TableConfig tableConfig, long estimatedRowSize) {
    if (tableConfig.fetchSize() != null) {
      return tableConfig.fetchSize();
    }

    try {
      if (estimatedRowSize == 0) {
        LOG.warn("Estimated row size is 0 for table {}, using default fetch size", tableConfig.tableName());
        return DEFAULT_FETCH_SIZE;
      }

      long workerMemory = getWorkerMemory();
      int workerCores = getWorkerCores();

      // Formula: (Memory of Dataflow worker VM) / (2 * 2 * (Number of cores on the
      // Dataflow worker VM) * (Maximum row size))
      // 2 * 2 = 4 (Safety factor)
      long denominator = 4L * workerCores * estimatedRowSize;

      if (denominator == 0) { // Should not happen given maxRowSize check and cores >= 1
        return DEFAULT_FETCH_SIZE;
      }

      long calculatedFetchSize = workerMemory / denominator;

      LOG.info("Auto-inferred fetch size for table {}: {} (Memory: {} bytes, Cores: {}, RowSize: {} bytes)",
          tableConfig.tableName(), calculatedFetchSize, workerMemory, workerCores, estimatedRowSize);

      if (calculatedFetchSize < MIN_FETCH_SIZE) {
        return MIN_FETCH_SIZE;
      }
      if (calculatedFetchSize > MAX_FETCH_SIZE) {
        return MAX_FETCH_SIZE;
      }

      return (int) calculatedFetchSize;

    } catch (Exception e) {
      LOG.warn("Failed to auto-infer fetch size for table {}, using default. Error: {}", tableConfig.tableName(),
          e.getMessage());
      return DEFAULT_FETCH_SIZE;
    }
  }

  @VisibleForTesting
  static long getWorkerMemory() {
    return Runtime.getRuntime().maxMemory();
  }

  @VisibleForTesting
  static int getWorkerCores() {
    return Runtime.getRuntime().availableProcessors();
  }
}
