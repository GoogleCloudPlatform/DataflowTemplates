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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.SpannerShard;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SpannerShardFileReader}. */
@RunWith(JUnit4.class)
public class SpannerShardFileReaderTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void testGetSpannerShards_validJson() throws IOException {
    String json =
        "[\n"
            + "  {\n"
            + "    \"projectId\": \"p1\",\n"
            + "    \"instanceId\": \"i1\",\n"
            + "    \"databaseId\": \"d1\"\n"
            + "  }\n"
            + "]";
    Path path = tmp.newFile("shards.json").toPath();
    Files.write(path, json.getBytes(StandardCharsets.UTF_8));

    SpannerShardFileReader reader = new SpannerShardFileReader();
    List<Shard> shards = reader.getSpannerShards(path.toString());

    assertNotNull(shards);
    assertEquals(1, shards.size());
    assertTrue(shards.get(0) instanceof SpannerShard);
    SpannerShard s = (SpannerShard) shards.get(0);
    assertEquals("p1", s.getProjectId());
    assertEquals("i1", s.getInstanceId());
    assertEquals("d1", s.getDatabaseId());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSpannerShards_multipleShardsThrows() throws IOException {
    String json =
        "[\n"
            + "  {\n"
            + "    \"projectId\": \"p1\",\n"
            + "    \"instanceId\": \"i1\",\n"
            + "    \"databaseId\": \"d1\"\n"
            + "  },\n"
            + "  {\n"
            + "    \"projectId\": \"p2\",\n"
            + "    \"instanceId\": \"i2\",\n"
            + "    \"databaseId\": \"d2\"\n"
            + "  }\n"
            + "]";
    Path path = tmp.newFile("multi-shards.json").toPath();
    Files.write(path, json.getBytes(StandardCharsets.UTF_8));

    SpannerShardFileReader reader = new SpannerShardFileReader();
    reader.getSpannerShards(path.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSpannerShards_emptyShardsThrows() throws IOException {
    String json = "[]";
    Path path = tmp.newFile("empty-shards.json").toPath();
    Files.write(path, json.getBytes(StandardCharsets.UTF_8));

    SpannerShardFileReader reader = new SpannerShardFileReader();
    reader.getSpannerShards(path.toString());
  }

  @Test(expected = RuntimeException.class)
  public void testGetSpannerShards_missingField() throws IOException {
    String json = "[{\"projectId\": \"p1\"}]"; // missing instance and database
    Path path = tmp.newFile("invalid.json").toPath();
    Files.write(path, json.getBytes(StandardCharsets.UTF_8));

    SpannerShardFileReader reader = new SpannerShardFileReader();
    reader.getSpannerShards(path.toString());
  }

  @Test(expected = RuntimeException.class)
  public void testGetSpannerShards_malformedJson() throws IOException {
    String json = "not-json";
    Path path = tmp.newFile("malformed.json").toPath();
    Files.write(path, json.getBytes(StandardCharsets.UTF_8));

    SpannerShardFileReader reader = new SpannerShardFileReader();
    reader.getSpannerShards(path.toString());
  }

  @Test(expected = RuntimeException.class)
  public void testGetSpannerShards_ioError() {
    SpannerShardFileReader reader = new SpannerShardFileReader();
    reader.getSpannerShards("/non/existent/path.json");
  }
}
