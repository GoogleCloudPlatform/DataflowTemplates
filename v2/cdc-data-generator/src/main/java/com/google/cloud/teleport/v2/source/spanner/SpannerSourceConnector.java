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
package com.google.cloud.teleport.v2.source.spanner;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.teleport.v2.source.ISourceConnector;
import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import com.google.cloud.teleport.v2.templates.sink.SinkSchemaFetcher;
import com.google.cloud.teleport.v2.templates.sink.TypeMapper;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Spanner implementation of {@link ISourceConnector}. */
public class SpannerSourceConnector implements ISourceConnector {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerSourceConnector.class);

  @VisibleForTesting
  public interface DialectProvider {
    Dialect getDialect(String projectId, String instanceId, String databaseId);
  }

  private static final DialectProvider DEFAULT_DIALECT_PROVIDER =
      (projectId, instanceId, databaseId) -> {
        SpannerOptions options = SpannerOptions.newBuilder().setProjectId(projectId).build();
        try (Spanner spanner = options.getService()) {
          return spanner.getDatabaseAdminClient().getDatabase(instanceId, databaseId).getDialect();
        }
      };

  private static DialectProvider dialectProvider = DEFAULT_DIALECT_PROVIDER;

  @VisibleForTesting
  public static void setDialectProvider(DialectProvider provider) {
    dialectProvider = provider != null ? provider : DEFAULT_DIALECT_PROVIDER;
  }

  @Override
  public String getSourceType() {
    return "SPANNER";
  }

  @Override
  public SinkType getSinkType() {
    return SinkType.SPANNER;
  }

  @Override
  public SinkConfig parseConfig(String sinkOptionsPath) throws IOException {
    if (sinkOptionsPath == null || sinkOptionsPath.isEmpty()) {
      throw new IllegalArgumentException("Sink options path must not be null or empty.");
    }

    SpannerConfigFileReader reader = new SpannerConfigFileReader();
    SpannerSinkConfig config = reader.getSpannerConfig(sinkOptionsPath);

    String projectId = config.getProjectId();
    if (projectId == null || projectId.isEmpty()) {
      throw new IllegalArgumentException("Missing projectId in Spanner configuration.");
    }
    String instanceId = config.getInstanceId();
    if (instanceId == null || instanceId.isEmpty()) {
      throw new IllegalArgumentException("Missing instanceId in Spanner configuration.");
    }

    String databaseId = config.getDatabaseId();
    if (databaseId == null || databaseId.isEmpty()) {
      throw new IllegalArgumentException("Missing databaseId in Spanner configuration.");
    }

    Dialect dialect = dialectProvider.getDialect(projectId, instanceId, databaseId);
    LOG.info("Pre-fetched Spanner dialect on driver: {}", dialect);

    config.setDialect(dialect);
    return config;
  }

  @Override
  public SinkSchemaFetcher getSchemaFetcher() {
    return new SpannerSchemaFetcher();
  }

  @Override
  public TypeMapper getTypeMapper() {
    return new SpannerTypeMapper();
  }

  @Override
  public DataWriter createDataWriter(SinkConfig sinkConfig) {
    if (!(sinkConfig instanceof SpannerSinkConfig)) {
      throw new IllegalArgumentException(
          "Sink configuration must be an instance of SpannerSinkConfig for Spanner sink type.");
    }
    return new SpannerDataWriter((SpannerSinkConfig) sinkConfig);
  }

  @Override
  public int resolveKeyParallelism(SinkConfig sinkConfig) {
    // High parallelism target for Spanner, representing a baseline of 500 DoFn across
    // a hypothetical maximum of 200 active workers to maximize horizontal write scaling.
    return 100000;
  }

  @Override
  public List<String> getLogicalShardIds(SinkConfig sinkConfig) {
    return Collections.emptyList();
  }
}
