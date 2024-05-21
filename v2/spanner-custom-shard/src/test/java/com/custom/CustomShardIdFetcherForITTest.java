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
package com.custom;

import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.v2.spanner.utils.ShardIdRequest;
import com.google.cloud.teleport.v2.spanner.utils.ShardIdResponse;
import java.util.Map;
import org.junit.Test;

/** Tests for CustomShardIdFetcherForIT class. */
public class CustomShardIdFetcherForITTest {
  @Test
  public void testGetShardId() {
    CustomShardIdFetcherForIT customShardIdFetcher = new CustomShardIdFetcherForIT();
    Map<String, Object> row = Map.of("SingerId", 2L);
    ShardIdResponse actual = customShardIdFetcher.getShardId(new ShardIdRequest("table1", row));
    ShardIdResponse expected = new ShardIdResponse();
    expected.setLogicalShardId("testShardB");
    assertEquals(actual.getLogicalShardId(), expected.getLogicalShardId());
    row = Map.of("SingerId", 1L);
    actual = customShardIdFetcher.getShardId(new ShardIdRequest("table1", row));
    expected.setLogicalShardId("testShardA");
    assertEquals(actual.getLogicalShardId(), expected.getLogicalShardId());
  }
}
