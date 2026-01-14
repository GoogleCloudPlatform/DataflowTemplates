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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class FetchSizeCalculatorTest {

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
  public void testGetFetchSize_WithMachineType() {
    TableConfig config =
        TableConfig.builder("t1").setFetchSize(null).setApproxRowCount(100L).build();

    // n1-standard-1: 3.75GB RAM, 1 vCPU
    Integer fetchSize = FetchSizeCalculator.getFetchSize(config, 1_000_000L, "n1-standard-1");
    assertNotNull(fetchSize);
    assertTrue(fetchSize > 100);
  }
}
