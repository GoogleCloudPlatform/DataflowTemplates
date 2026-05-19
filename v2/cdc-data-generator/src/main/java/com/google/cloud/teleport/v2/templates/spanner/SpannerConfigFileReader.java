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
package com.google.cloud.teleport.v2.templates.spanner;

import com.google.cloud.teleport.v2.templates.model.SpannerSinkConfig;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to read the Cloud Spanner configuration file in GCS and convert it into a SpannerSinkConfig
 * object.
 */
public class SpannerConfigFileReader {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerConfigFileReader.class);

  public SpannerSinkConfig getSpannerConfig(String spannerConfigFilePath) {
    try (InputStream stream =
        Channels.newInputStream(
            FileSystems.open(FileSystems.matchNewResource(spannerConfigFilePath, false)))) {

      String result = IOUtils.toString(stream, StandardCharsets.UTF_8);
      SpannerSinkConfig config =
          new GsonBuilder()
              .setFieldNamingPolicy(FieldNamingPolicy.IDENTITY)
              .create()
              .fromJson(result, SpannerSinkConfig.class);

      if (config == null) {
        throw new RuntimeException(
            "Failed to parse Spanner configuration: resulting config is null.");
      }

      if (config.getProjectId() == null || config.getProjectId().isEmpty()) {
        throw new IllegalArgumentException(
            "Missing projectId in Spanner configuration file: " + spannerConfigFilePath);
      }
      if (config.getInstanceId() == null || config.getInstanceId().isEmpty()) {
        throw new IllegalArgumentException(
            "Missing instanceId in Spanner configuration file: " + spannerConfigFilePath);
      }
      if (config.getDatabaseId() == null || config.getDatabaseId().isEmpty()) {
        throw new IllegalArgumentException(
            "Missing databaseId in Spanner configuration file: " + spannerConfigFilePath);
      }

      LOG.info("Read Spanner configuration: {}", config);
      return config;

    } catch (IOException e) {
      LOG.error(
          "Failed to read Spanner configuration file. Make sure it is ASCII or UTF-8 encoded and"
              + " contains a well-formed JSON string.",
          e);
      throw new RuntimeException(
          "Failed to read Spanner configuration file. Make sure it is ASCII or UTF-8 encoded and"
              + " contains a well-formed JSON string.",
          e);
    }
  }
}
