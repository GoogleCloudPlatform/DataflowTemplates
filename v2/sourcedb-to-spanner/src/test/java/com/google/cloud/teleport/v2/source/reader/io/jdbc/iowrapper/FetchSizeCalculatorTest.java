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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.TableConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class FetchSizeCalculatorTest {

  private TableConfig tableConfig;

  @Before
  public void setUp() {
    tableConfig = TableConfig.builder("t1").setApproxRowCount(100L).build();
  }

  @Test
  public void testGetFetchSize_NoMachineType() {
    // Test when machine type (memory/cores) is not provided.
    int fetchSize = FetchSizeCalculator.getFetchSize(tableConfig, 100L, null, null);
    assertEquals(0, fetchSize);
  }

  @Test
  public void testGetFetchSize_ExplicitFetchSize() {
    // Test when fetch size is explicitly configured in TableConfig.
    TableConfig configWithFetchSize = TableConfig.builder("t1").setFetchSize(12345).build();
    int fetchSize = FetchSizeCalculator.getFetchSize(configWithFetchSize, 100L, null, null);
    assertEquals(12345, fetchSize);
  }

  @Test
  public void testGetFetchSize_ZeroRowSize() {
    // Test when estimated row size is 0.
    int fetchSize = FetchSizeCalculator.getFetchSize(tableConfig, 0L, 16.0, 4);
    assertEquals(0, fetchSize);
  }

  @Test
  public void testGetFetchSize_StandardCalculation() {
    // Test standard calculation.
    // Memory: 16 GB = 17,179,869,184 bytes
    // Cores: 4
    // Row Size: 1000 bytes
    // Denominator = 4 * 4 * 1000 = 16,000
    // Expected Fetch Size = 17,179,869,184 / 16,000 = 1,073,741

    int fetchSize = FetchSizeCalculator.getFetchSize(tableConfig, 1000L, 16.0, 4);
    assertEquals(1073741, fetchSize);
  }

  @Test
  public void testGetFetchSize_SmallRowSize() {
    // Test with small row size (should result in large fetch size, capped at
    // MAX_INTEGER if implemented,
    // but here check calculation logic).
    // Memory: 16 GB
    // Cores: 4
    // Row Size: 10 bytes
    // Denominator = 4 * 4 * 10 = 160
    // Expected Fetch Size = 17,179,869,184 / 160 = 107,374,182
    // Integer.MAX_VALUE = 2,147,483,647. Result is within integer range.

    int fetchSize = FetchSizeCalculator.getFetchSize(tableConfig, 10L, 16.0, 4);
    assertEquals(107374182, fetchSize);
  }

  @Test
  public void testGetFetchSize_LargeRowSize() {
    // Test with large row size.
    // Memory: 16 GB
    // Cores: 4
    // Row Size: 10 MB (10,485,760 bytes)
    // Denominator = 4 * 4 * 10,485,760 = 167,772,160
    // Expected Fetch Size = 17,179,869,184 / 167,772,160 = 102

    int fetchSize = FetchSizeCalculator.getFetchSize(tableConfig, 10485760L, 16.0, 4);
    assertEquals(102, fetchSize);
  }

  @Test
  public void testGetFetchSize_ZeroCores() {
    // Test when cores are 0 (should typically be handled by Utils returning null or
    // >=1, but testing calculator logic).
    // In this case, providing 0 cores.
    int fetchSize = FetchSizeCalculator.getFetchSize(tableConfig, 100L, 16.0, 0);
    assertEquals(0, fetchSize);
  }

  @Test
  public void testGetFetchSize_nullInputs() {
    // Null memory
    assertEquals(0, (int) FetchSizeCalculator.getFetchSize(tableConfig, 100L, null, 4));
    // Null cores
    assertEquals(0, (int) FetchSizeCalculator.getFetchSize(tableConfig, 100L, 16.0, null));
  }

  @Test
  public void testGetFetchSize_LessThanMinFetchSize() {
    // Memory: 1 GB (1,073,741,824 bytes)
    // Cores: 100
    // Row Size: 10,000,000 bytes
    // Denominator = 4 * 100 * 10,000,000 = 4,000,000,000
    // Expected Fetch Size is 0, which gets capped up to 1 (MIN_FETCH_SIZE)
    int fetchSize = FetchSizeCalculator.getFetchSize(tableConfig, 10000000L, 1.0, 100);
    assertEquals(1, fetchSize);
  }

  @Test
  public void testGetFetchSize_GreaterThanMaxFetchSize() {
    // Memory: 100 GB
    // Cores: 1
    // Row Size: 1 byte
    // Expected Fetch Size cap to Integer.MAX_VALUE
    int fetchSize = FetchSizeCalculator.getFetchSize(tableConfig, 1L, 100.0, 1);
    assertEquals(Integer.MAX_VALUE, fetchSize);
  }

  @Test
  public void testGetFetchSize_ExceptionThrown() {
    TableConfig mockTableConfig = org.mockito.Mockito.mock(TableConfig.class);
    org.mockito.Mockito.when(mockTableConfig.fetchSize()).thenReturn(null);
    org.mockito.Mockito.when(mockTableConfig.tableName())
        .thenThrow(new RuntimeException("Mock Exception"))
        .thenReturn("mock_table");

    int fetchSize = FetchSizeCalculator.getFetchSize(mockTableConfig, 100L, 16.0, 4);
    assertEquals(0, fetchSize);
  }
}
