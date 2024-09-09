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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.cloud.teleport.v2.spanner.migrations.shard.ShardingContext;
import com.google.common.io.Resources;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;

/** Tests for ShardingContextReader class. */
public class ShardingContextReaderTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void readShardingContextFile() {
    Path shardingContextPath = Paths.get(Resources.getResource("sharding-context.json").getPath());
    ShardingContext shardingContext =
        ShardingContextReader.getShardingContext(shardingContextPath.toString());
    ShardingContext expectedShardingContext = getShardingContextObject();
    // Validates that the sharding context object created is correct.
    assertThat(
        shardingContext.getStreamToDbAndShardMap(),
        is(expectedShardingContext.getStreamToDbAndShardMap()));
  }

  private static ShardingContext getShardingContextObject() {
    Map<String, Map<String, String>> schemaToDbAndShardMap = new HashMap<>();
    Map<String, String> schemaToShardId = new HashMap<>();
    schemaToShardId.put("shard1", "id1");
    schemaToShardId.put("shard2", "id2");
    schemaToDbAndShardMap.put("stream1", schemaToShardId);
    schemaToDbAndShardMap.put("stream2", schemaToShardId);
    ShardingContext expectedShardingContext = new ShardingContext(schemaToDbAndShardMap);
    return expectedShardingContext;
  }

  @Test
  public void readEmptyShardingContextFilePath() {
    String shardingContextPath = null;
    ShardingContext shardingContext = ShardingContextReader.getShardingContext(shardingContextPath);
    ShardingContext expectedShardingContext = new ShardingContext();
    // Validates that the sharding context object created is correct.
    assertThat(
        shardingContext.getStreamToDbAndShardMap(),
        is(expectedShardingContext.getStreamToDbAndShardMap()));
  }
}
