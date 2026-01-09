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
    int fetchSize = FetchSizeCalculator.getFetchSize(config, 100L);
    assertTrue(fetchSize >= 100);
    assertTrue(fetchSize <= 100_000);
  }

  @Test
  public void testGetFetchSize_Explicit() {
    TableConfig config =
        TableConfig.builder("t1").setFetchSize(12345).setApproxRowCount(100L).build();
    // Row size doesn't matter for explicit
    int fetchSize = FetchSizeCalculator.getFetchSize(config, 100L);
    assertEquals(12345, fetchSize);
  }

  @Test
  public void testGetFetchSize_ZeroRowSize() {
    TableConfig config =
        TableConfig.builder("t1").setFetchSize(null).setApproxRowCount(100L).build();
    int fetchSize = FetchSizeCalculator.getFetchSize(config, 0L);
    // Expected to return default fetch size (50_000)
    assertEquals(50_000, fetchSize);
  }
}
