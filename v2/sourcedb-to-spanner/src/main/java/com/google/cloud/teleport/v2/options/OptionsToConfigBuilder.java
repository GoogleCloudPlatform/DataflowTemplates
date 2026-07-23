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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.v2.reader.auth.dbauth.LocalCredentialsProvider;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.SourceConnectorFactory;
import com.google.cloud.teleport.v2.source.jdbc.AbstractJdbcSrcToSpSourceConnector;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.DataflowWorkerMachineTypeUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OptionsToConfigBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(OptionsToConfigBuilder.class);
  public static final String DEFAULT_POSTGRESQL_NAMESPACE = "public";

  /**
   * Extracts the worker zone from the options.
   *
   * @param options Pipeline options.
   * @return The worker zone or null if not found.
   */
  public static String extractWorkerZone(PipelineOptions options) {
    try {
      return options.as(DataflowPipelineWorkerPoolOptions.class).getWorkerZone();
    } catch (Exception e) {
      LOG.warn("Could not extract worker zone from options. Defaulting to null.", e);
      return null;
    }
  }

  public static JdbcIOWrapperConfig getJdbcIOWrapperConfigWithDefaults(
      SourceDbToSpannerOptions options, Shard shard, List<String> tables, Wait.OnSignal<?> waitOn) {
    SQLDialect sqlDialect = SQLDialect.valueOf(options.getSourceDbDialect());

    String jdbcDriverClassName = options.getJdbcDriverClassName();
    String jdbcDriverJars = options.getJdbcDriverJars();
    long maxConnections =
        options.getMaxConnections() > 0 ? (long) (options.getMaxConnections()) : 0;
    Integer numPartitions = options.getNumPartitions();
    String workerZone = extractWorkerZone(options);

    Integer fetchSize = options.getFetchSize();
    if (fetchSize != null && fetchSize < 0) {
      fetchSize = null;
    }

    return getJdbcIOWrapperConfig(
        sqlDialect,
        tables,
        shard,
        jdbcDriverClassName,
        jdbcDriverJars,
        maxConnections,
        numPartitions,
        waitOn,
        fetchSize,
        options.getUniformizationStageCountHint(),
        options.getProjectId(),
        workerZone,
        options.as(DataflowPipelineWorkerPoolOptions.class).getWorkerMachineType());
  }

  public static JdbcIOWrapperConfig getJdbcIOWrapperConfig(
      SQLDialect sqlDialect,
      List<String> tables,
      Shard shard,
      String jdbcDriverClassName,
      String jdbcDriverJars,
      long maxConnections,
      Integer numPartitions,
      Wait.OnSignal<?> waitOn,
      Integer fetchSize,
      Long uniformizationStageCountHint,
      String projectId,
      String workerZone,
      String workerMachineType) {
    AbstractJdbcSrcToSpSourceConnector connector =
        SourceConnectorFactory.getSourceJdbcConnectorByDialect(sqlDialect);
    JdbcIOWrapperConfig.Builder builder = connector.getJdbcIOWrapperConfigBuilder();
    SourceSchemaReference sourceSchemaReference =
        connector.getSourceSchemaReference(shard.getDbName(), shard.getNamespace());
    builder =
        builder
            .setSourceSchemaReference(sourceSchemaReference)
            .setDbAuth(
                LocalCredentialsProvider.builder()
                    .setUserName(
                        shard.getUserName()) // TODO - support taking username and password from url
                    // as well
                    .setPassword(shard.getPassword())
                    .build())
            .setJdbcDriverClassName(jdbcDriverClassName)
            .setJdbcDriverJars(jdbcDriverJars);

    if (workerMachineType != null && !workerMachineType.isEmpty()) {
      builder.setWorkerMemoryBytes(
          DataflowWorkerMachineTypeUtils.getWorkerMemoryBytes(
              projectId, workerZone, workerMachineType));
      builder.setWorkerCores(
          DataflowWorkerMachineTypeUtils.getWorkerCores(projectId, workerZone, workerMachineType));
    }
    if (maxConnections != 0) {
      builder = builder.setMaxConnections(maxConnections);
    }

    String sourceDbURL =
        connector.getJdbcUrl(
            shard.getHost(),
            Integer.parseInt(shard.getPort()),
            shard.getDbName(),
            shard.getConnectionProperties(),
            shard.getNamespace(),
            fetchSize);

    builder.setSourceDbURL(sourceDbURL);
    if (!StringUtils.isEmpty(shard.getLogicalShardId())) {
      builder.setShardID(shard.getLogicalShardId());
    }

    if (waitOn != null) {
      builder.setWaitOn(waitOn);
    }

    builder.setMaxPartitions(numPartitions);
    builder = builder.setTables(ImmutableList.copyOf(tables));
    builder = builder.setMaxFetchSize(fetchSize);
    builder = builder.setSplitStageCountHint(uniformizationStageCountHint);
    return builder.build();
  }

  @VisibleForTesting
  public static String addParamToJdbcUrl(String jdbcUrl, String paramName, String paramValue) {
    // URI/ URL libraries don't seem to handle jdbc URLs well
    Pattern queryPattern = Pattern.compile("\\?(.*?)$");

    Matcher matcher = queryPattern.matcher(jdbcUrl);

    String baseUrl;
    String query;

    if (matcher.find()) {
      baseUrl = jdbcUrl.substring(0, matcher.start());
      query = matcher.group(1);
    } else {
      baseUrl = jdbcUrl;
      query = null;
    }

    if (query == null) {
      // No parameters exist, add the new one
      return jdbcUrl + "?" + paramName + "=" + paramValue;
    } else {
      String[] params = query.split("&");
      boolean paramFound = false;
      StringBuilder newQuery = new StringBuilder();

      for (String param : params) {
        String[] keyValue = param.split("=");
        if (keyValue[0].equals(paramName)) {
          paramFound = true;
          if (keyValue[1].equals(paramValue)) {
            // Parameter exists with the correct value, keep it as is
            if (newQuery.length() > 0) {
              newQuery.append("&");
            }
            newQuery.append(param);
          } else {
            // Mismatch, handle based on the mismatchException flag
            throw new IllegalArgumentException(
                "Parameter mismatch for "
                    + paramName
                    + " URL = "
                    + jdbcUrl
                    + " config value = "
                    + paramValue);
          }
        } else {
          if (newQuery.length() > 0) {
            newQuery.append("&");
          }
          newQuery.append(param);
        }
      }

      if (!paramFound) {
        // Parameter doesn't exist, add it
        if (newQuery.length() > 0) {
          newQuery.append("&");
        }
        newQuery.append(paramName).append("=").append(paramValue);
      }

      return baseUrl + "?" + newQuery;
    }
  }

  private static String extractDbFromURL(String sourceDbUrl) {
    URI uri;
    try {
      // Strip off the prefix 'jdbc:' which the library cannot handle.
      uri = new URI(sourceDbUrl.substring(5));
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Unable to parse url: %s", sourceDbUrl), e);
    }
    // Remove '/' before returning.
    return uri.getPath().substring(1);
  }

  private OptionsToConfigBuilder() {}
}
