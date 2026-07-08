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

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.SpannerShard;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads a JSON array of Spanner shard configurations from GCS and returns a list of {@link
 * SpannerShard} instances.
 *
 * <p>Each entry in the JSON file must contain the following fields:
 *
 * <pre>
 * [
 *   {
 *     "projectId": "my-gcp-project",
 *     "instanceId": "my-spanner-instance",
 *     "databaseId": "my-database"
 *   }
 * ]
 * </pre>
 */
public class SpannerShardFileReader {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerShardFileReader.class);

  /**
   * Reads Spanner shard configuration from the given GCS file path.
   *
   * @param shardsFilePath GCS path to the JSON shard config file.
   * @return list of {@link SpannerShard} objects, sorted by logicalShardId.
   */
  public List<Shard> getSpannerShards(String shardsFilePath) {
    try (InputStream stream =
        Channels.newInputStream(
            FileSystems.open(FileSystems.matchNewResource(shardsFilePath, false)))) {

      String result = IOUtils.toString(stream, StandardCharsets.UTF_8);
      Type listType = new TypeToken<List<Map<String, String>>>() {}.getType();
      List<Map<String, String>> shardConfigs =
          new GsonBuilder()
              .setFieldNamingPolicy(FieldNamingPolicy.IDENTITY)
              .create()
              .fromJson(result, listType);

      List<Shard> shards = new ArrayList<>();
      for (Map<String, String> config : shardConfigs) {
        String projectId = config.get("projectId");
        String instanceId = config.get("instanceId");
        String databaseId = config.get("databaseId");
        if (projectId == null || instanceId == null || databaseId == null) {
          throw new RuntimeException(
              "SpannerShard config at '"
                  + shardsFilePath
                  + "' is missing one or more required fields: projectId, instanceId, databaseId");
        }
        shards.add(new SpannerShard(projectId, instanceId, databaseId));
        LOG.info(
            "Loaded SpannerShard: project={}, instance={}, database={}",
            projectId,
            instanceId,
            databaseId);
      }

      if (shards.size() != 1) {
        throw new IllegalArgumentException(
            "For Spanner targets, exactly 1 shard must be specified in the configuration file. "
                + "Found: "
                + shards.size());
      }

      LOG.info("Read {} Spanner shard from {}", shards.size(), shardsFilePath);
      return shards;

    } catch (IOException e) {
      throw new RuntimeException("Failed to read Spanner shard config file: " + shardsFilePath, e);
    }
  }
}
