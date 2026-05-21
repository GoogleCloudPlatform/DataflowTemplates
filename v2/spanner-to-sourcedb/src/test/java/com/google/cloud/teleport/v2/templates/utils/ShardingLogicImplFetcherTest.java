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
package com.google.cloud.teleport.v2.templates.utils;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.utils.IShardIdFetcher;
import org.junit.Before;
import org.junit.Test;

public class ShardingLogicImplFetcherTest {

  @Before
  public void setUp() {
    ShardingLogicImplFetcher.reset();
  }

  @Test
  public void testGetShardingLogicImpl_Default() {
    ISchemaMapper mockSchemaMapper = mock(ISchemaMapper.class);
    IShardIdFetcher fetcher =
        ShardingLogicImplFetcher.getShardingLogicImpl("", "", "", mockSchemaMapper, "skip");
    assertTrue(fetcher instanceof ShardIdFetcherImpl);
  }

  @Test(expected = RuntimeException.class)
  public void testGetShardingLogicImpl_Custom_Failure() {
    ISchemaMapper mockSchemaMapper = mock(ISchemaMapper.class);
    // This should fail because the jar path and class name are invalid
    ShardingLogicImplFetcher.getShardingLogicImpl(
        "invalid.jar", "InvalidClass", "", mockSchemaMapper, "skip");
  }
}
