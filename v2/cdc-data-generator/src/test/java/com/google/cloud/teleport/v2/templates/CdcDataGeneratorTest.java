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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.MySqlSinkConfig;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

public class CdcDataGeneratorTest {

  @Test
  public void testOptionsParsing() {
    String[] args = {"--sinkType=SPANNER", "--sinkOptions={\"projectId\": \"p\"}"};

    CdcDataGeneratorOptions options =
        PipelineOptionsFactory.fromArgs(args).as(CdcDataGeneratorOptions.class);

    assertEquals(SinkType.SPANNER, options.getSinkType());
    assertEquals("{\"projectId\": \"p\"}", options.getSinkOptions());
  }

  @Test
  public void testResolveKeyParallelism_spanner() {
    int parallelism = CdcDataGenerator.resolveKeyParallelism(SinkType.SPANNER, null);
    assertEquals(50000, parallelism);
  }

  @Test
  public void testResolveKeyParallelism_mysql_singleShard() {
    MySqlSinkConfig config =
        new MySqlSinkConfig(
            Collections.singletonList(
                new Shard("shardA", "host", "user", "pass", "port", "db", null, null, null)));
    int parallelism = CdcDataGenerator.resolveKeyParallelism(SinkType.MYSQL, config);
    assertEquals(5000, parallelism);
  }

  @Test
  public void testResolveKeyParallelism_mysql_multipleShards() {
    MySqlSinkConfig config =
        new MySqlSinkConfig(
            Arrays.asList(
                new Shard("shardA", "host", "user", "pass", "port", "db", null, null, null),
                new Shard("shardB", "host", "user", "pass", "port", "db", null, null, null),
                new Shard("shardC", "host", "user", "pass", "port", "db", null, null, null)));
    int parallelism = CdcDataGenerator.resolveKeyParallelism(SinkType.MYSQL, config);
    assertEquals(15000, parallelism);
  }

  @Test
  public void testResolveKeyParallelism_mysql_emptyOrNullShards() {
    MySqlSinkConfig configWithNullShards = new MySqlSinkConfig(null);
    assertEquals(
        5000, CdcDataGenerator.resolveKeyParallelism(SinkType.MYSQL, configWithNullShards));

    MySqlSinkConfig configWithEmptyShards = new MySqlSinkConfig(Collections.emptyList());
    assertEquals(
        5000, CdcDataGenerator.resolveKeyParallelism(SinkType.MYSQL, configWithEmptyShards));
  }

  @Test
  public void testResolveKeyParallelism_mysql_invalidConfigType() {
    SinkConfig invalidConfig = new SinkConfig() {};
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> CdcDataGenerator.resolveKeyParallelism(SinkType.MYSQL, invalidConfig));
    assertEquals(
        "Sink configuration must be an instance of MySqlSinkConfig for MySQL sink type.",
        ex.getMessage());
  }

  @Test
  public void testResolveKeyParallelism_unsupportedSinkType() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> CdcDataGenerator.resolveKeyParallelism(null, null));
    assertEquals("Unsupported sink type: null", ex.getMessage());
  }
}
