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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.TableConfig;
import com.google.cloud.teleport.v2.spanner.migrations.utils.DataflowWorkerMachineTypeUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class FetchSizeCalculatorTest {

  @org.junit.Before
  public void setUp() throws Exception {
    resetCache();
  }

  @org.junit.After
  public void tearDown() throws Exception {
    resetCache();
  }

  private void resetCache() throws Exception {
    java.lang.reflect.Method method =
        DataflowWorkerMachineTypeUtils.class.getDeclaredMethod("resetCacheForTesting");
    method.setAccessible(true);
    method.invoke(null);
  }

  private void putMachineSpec(String machineType, double memoryGB, int vCPUs) throws Exception {
    java.lang.reflect.Method method =
        DataflowWorkerMachineTypeUtils.class.getDeclaredMethod(
            "putMachineSpecForTesting", String.class, double.class, int.class);
    method.setAccessible(true);
    method.invoke(null, machineType, memoryGB, vCPUs);
  }

  @Test
  public void testGetFetchSize_NoMachineType() {
    TableConfig config =
        TableConfig.builder("t1").setFetchSize(null).setApproxRowCount(100L).build();
    // No machine type offered, so calculation should be impossible
    Integer fetchSize = FetchSizeCalculator.getFetchSize(config, 100L, null);
    assertNull(fetchSize);
  }

  @Test
  public void testGetFetchSize_Explicit() {
    TableConfig config =
        TableConfig.builder("t1").setFetchSize(12345).setApproxRowCount(100L).build();
    // Row size doesn't matter for explicit
    Integer fetchSize = FetchSizeCalculator.getFetchSize(config, 100L, null);
    assertEquals(Integer.valueOf(12345), fetchSize);
  }

  @Test
  public void testGetFetchSize_ZeroRowSize() {
    TableConfig config =
        TableConfig.builder("t1").setFetchSize(null).setApproxRowCount(100L).build();
    Integer fetchSize = FetchSizeCalculator.getFetchSize(config, 0L, null);
    // Zero row size means we can't safely estimate memory usage -> return null
    assertNull(fetchSize);
  }

  @Test
  public void testGetFetchSize_SchemaMapperOverride() {
    TableConfig config =
        TableConfig.builder("t1")
            .setFetchSize(42) // User override
            .build();
    // Even if row size is huge, the override should be respected exactly.
    Integer fetchSize = FetchSizeCalculator.getFetchSize(config, 100_000_000L, null);
    assertEquals(Integer.valueOf(42), fetchSize);
  }

  @Test
  public void testGetFetchSize_WithMachineType() throws Exception {
    TableConfig config =
        TableConfig.builder("t1").setFetchSize(null).setApproxRowCount(100L).build();

    // n1-standard-1: 3.75GB RAM, 1 vCPU
    putMachineSpec("n1-standard-1", 3.75, 1);
    Integer fetchSize = FetchSizeCalculator.getFetchSize(config, 1_000_000L, "n1-standard-1");
    assertNotNull(fetchSize);
    assertTrue(fetchSize > 0);
  }

  @Test
  public void testGetFetchSize_ClampedToMax() throws Exception {
    TableConfig config =
        TableConfig.builder("t1").setFetchSize(null).setApproxRowCount(100L).build();

    // custom-1-16384: 16GB RAM, 1 vCPU
    // Memory: ~17 billion bytes
    // Denominator: 4 * 1 * 1 = 4
    // Calculated: ~4.29 billion > Integer.MAX_VALUE
    putMachineSpec("custom-1-16384", 16.0, 1);
    Integer fetchSize = FetchSizeCalculator.getFetchSize(config, 1L, "custom-1-16384");
    assertNotNull(fetchSize);
    assertEquals(Integer.MAX_VALUE, (int) fetchSize);
  }

  @Test
  public void testGetFetchSize_WithProjectIdAndZone() throws Exception {
    TableConfig config =
        TableConfig.builder("t1").setFetchSize(null).setApproxRowCount(100L).build();

    // Verify that passing projectId and zone works correctly with the cache
    putMachineSpec("n1-standard-1", 3.75, 1);

    // We can't easily verify that projectId/zone were *used* by the cache lookup
    // (since cache lookup ignores them if key is found),
    // but we can verify the method returns a result without crashing.
    Integer fetchSize =
        FetchSizeCalculator.getFetchSize(
            config, 1_000_000L, "n1-standard-1", "dummy-project", "dummy-zone");
    assertNotNull(fetchSize);
  }
}
