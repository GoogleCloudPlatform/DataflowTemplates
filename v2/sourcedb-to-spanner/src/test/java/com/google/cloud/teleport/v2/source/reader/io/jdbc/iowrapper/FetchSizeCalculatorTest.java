/*
 * Copyright (C) 2025 Google LLC
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.TableConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class FetchSizeCalculatorTest {

  @Test
  public void testGetFetchSize_Defaults() {
    TableConfig config =
        TableConfig.builder("t1").setFetchSize(null).setApproxRowCount(100L).build();
    // Pass a dummy estimated row size
    int fetchSize = FetchSizeCalculator.getFetchSize(config, 100L, null);
    assertTrue(fetchSize >= 100);
    assertTrue(fetchSize <= 100_000);
  }

  @Test
  public void testGetFetchSize_Explicit() {
    TableConfig config =
        TableConfig.builder("t1").setFetchSize(12345).setApproxRowCount(100L).build();
    // Row size doesn't matter for explicit
    int fetchSize = FetchSizeCalculator.getFetchSize(config, 100L, null);
    assertEquals(12345, fetchSize);
  }

  @Test
  public void testGetFetchSize_ZeroRowSize() {
    TableConfig config =
        TableConfig.builder("t1").setFetchSize(null).setApproxRowCount(100L).build();
    int fetchSize = FetchSizeCalculator.getFetchSize(config, 0L, null);
    // Expected to return default fetch size (50_000)
    assertEquals(50_000, fetchSize);
  }

  @Test
  public void testGetFetchSize_SchemaMapperOverride() {
    TableConfig config =
        TableConfig.builder("t1")
            .setFetchSize(42) // User override
            .build();
    // Even if row size is huge, the override should be respected exactly.
    int fetchSize = FetchSizeCalculator.getFetchSize(config, 100_000_000L, null);
    assertEquals(42, fetchSize);
  }

  @Test
  public void testGetFetchSize_WithMachineType() {
    TableConfig config =
        TableConfig.builder("t1").setFetchSize(null).setApproxRowCount(100L).build();
    // n1-standard-1 has 3.75GB memory.
    // 3.75 * 1024^3 = 4,026,531,840 bytes.
    // Cores = 1 (DataflowWorkerMachineTypeUtils assumes 1 core for n1-standard-1,
    // wait, checks map)
    // Actually FetchSizeCalculator.getWorkerCores() uses
    // Runtime.availableProcessors().
    // This makes the test environment dependent if we don't mock getWorkerCores.
    // However, we can just check if it returns a reasonable value or mock it if
    // needed.
    // But since `getWorkerCores` is static and not easily mockable without
    // Powermock,
    // we will rely on the fact that we use getWorkerMemory which calls
    // DataflowWorkerMachineTypeUtils.

    // Let's use a very large row size to force a small fetch size, ensuring logic
    // is hit.
    // If we use n1-standard-1 (3.75GB), and row size 1MB.
    // Denom = 4 * Cores * 1MB.
    // If Cores = 1, Denom = 4MB.
    // Fetch = 3.75GB / 4MB ~= 960.
    // 960 is between MIN(100) and MAX(100000).

    int fetchSize = FetchSizeCalculator.getFetchSize(config, 1_000_000L, "n1-standard-1");
    assertTrue(fetchSize > 100);
    assertTrue(fetchSize < 100_000);
    // Rough check: 3.75GB / (4 * 16 * 1MB) if 16 cores machine running test?
    // We can't assert exact value easily without knowing cores.
  }
}
