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
package com.google.cloud.teleport.v2.spanner.migrations.source.config;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CassandraDriverConfigLoader;
import com.google.cloud.teleport.v2.spanner.migrations.utils.FileLoader;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ISecretManagerAccessor;
import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigResolveOptions;
import java.util.Comparator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses the source configuration from JSON/HOCON and resolves secrets from Secret Manager.
 *
 * <p>Supports the following source types: MySQL, PostgreSQL, Cassandra, and Astra DB.
 */
public class SourceConfigParser {
  private static final Logger LOG = LoggerFactory.getLogger(SourceConfigParser.class);
  private final ObjectMapper mapper;

  private final ISecretManagerAccessor secretManagerAccessor;

  /**
   * Constructs a new {@code SourceConfigParser} with the specified secret manager accessor.
   *
   * @param secretManagerAccessor the accessor used to resolve secrets from Secret Manager
   */
  public SourceConfigParser(ISecretManagerAccessor secretManagerAccessor) {
    this.secretManagerAccessor = secretManagerAccessor;
    this.mapper =
        new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper
        .configOverride(String.class)
        .setSetterInfo(JsonSetter.Value.forValueNulls(Nulls.AS_EMPTY));
  }

  /**
   * Parses the configuration file from GCS into the appropriate {@link SourceConnectionConfig}
   * implementing class.
   *
   * @param sourceTypeStr The source database type ("mysql", "postgresql", "cassandra", "astra_db").
   * @param sourceConfigFilePath The URI to the HOCON or JSON config file.
   * @return A populated implementation of {@link SourceConnectionConfig}.
   */
  public SourceConnectionConfig parseConfiguration(
      String sourceTypeStr, String sourceConfigFilePath) throws Exception {
    return parseConfiguration(sourceTypeStr, sourceConfigFilePath, /* resolveSecrets= */ true);
  }

  /**
   * Parses the configuration file from GCS into the appropriate {@link SourceConnectionConfig}
   * implementing class.
   *
   * @param sourceTypeStr The source database type ("mysql", "postgresql", "cassandra", "astra_db").
   * @param sourceConfigFilePath The URI to the HOCON or JSON config file.
   * @param resolveSecrets Whether to resolve secrets from Secret Manager.
   * @return A populated implementation of {@link SourceConnectionConfig}.
   */
  public SourceConnectionConfig parseConfiguration(
      String sourceTypeStr, String sourceConfigFilePath, boolean resolveSecrets) throws Exception {

    SourceType sourceType = SourceType.parseSourceType(sourceTypeStr);
    switch (SourceType.parseSourceType(sourceTypeStr)) {
      case CASSANDRA:
        // Maps directly to the DataStax OptionsMap
        return new CassandraConnectionConfig(
            CassandraDriverConfigLoader.getOptionsMapFromFile(sourceConfigFilePath));
      case ASTRA_DB:
        String astraFileContent = FileLoader.readConfigFilePath(sourceConfigFilePath);
        Map<String, Object> astraConfigMap = parseConfigToConfigMap(astraFileContent);
        return mapper.convertValue(astraConfigMap, AstraConnectionConfig.class);
      case MYSQL:
      case PG:
        String jdbcFileContent = FileLoader.readConfigFilePath(sourceConfigFilePath);
        Map<String, Object> jdbcConfigMap = parseConfigToConfigMap(jdbcFileContent);
        JdbcShardConfig jdbcShardConfig = mapper.convertValue(jdbcConfigMap, JdbcShardConfig.class);
        if (jdbcShardConfig.getShardConfigs() == null
            || jdbcShardConfig.getShardConfigs().isEmpty()) {
          throw new IllegalArgumentException(
              "The configuration file is missing the 'shardConfigs' field.");
        }
        // Returns ordered list of shards
        jdbcShardConfig.getShardConfigs().sort(Comparator.comparing(Shard::getLogicalShardId));
        if (resolveSecrets) {
          resolveShardSecret(jdbcShardConfig, sourceConfigFilePath);
        }
        return jdbcShardConfig;
      default:
        throw new IllegalArgumentException("Unsupported source type: " + sourceType);
    }
  }

  /**
   * Resolves password secrets for all shard configurations inside the given {@link JdbcShardConfig}
   * using the {@link ISecretManagerAccessor}.
   *
   * @param jdbcShardConfig the JDBC shard configuration object containing shard configs to be
   *     updated
   * @param sourceShardsFilePath the path to the source shards configuration file, used for error
   *     reporting
   * @throws RuntimeException if neither the password nor secretManagerUri is found for any shard
   */
  @VisibleForTesting
  void resolveShardSecret(JdbcShardConfig jdbcShardConfig, String sourceShardsFilePath) {
    if (jdbcShardConfig.getShardConfigs() == null || jdbcShardConfig.getShardConfigs().isEmpty()) {
      throw new IllegalArgumentException(
          "The configuration file is missing the 'shardConfigs' field.");
    }
    for (Shard shard : jdbcShardConfig.getShardConfigs()) {
      LOG.info("Processing shard: {}", shard.getLogicalShardId());
      String password =
          secretManagerAccessor.resolvePassword(
              shard.getSecretManagerUri(), shard.getLogicalShardId(), shard.getPassword());
      if (password == null || password.isEmpty()) {
        throw new RuntimeException(
            "Neither password nor secretManagerUri was found in the shard file "
                + sourceShardsFilePath
                + "  for shard "
                + shard.getLogicalShardId());
      }
      shard.setPassword(password);
    }
  }

  /**
   * Parses a configuration string (either HOCON or JSON format) and converts it into a standard
   * Java {@link Map}.
   *
   * @param configContent the HOCON or JSON configuration string
   * @return a map representing the resolved configuration properties
   */
  @VisibleForTesting
  static Map<String, Object> parseConfigToConfigMap(String configContent) {

    // Parse HOCON/JSON content
    // ConfigFactory.parseString handles both HOCON and JSON formats seamlessly.
    // resolve() handles HOCON's inbuilt inheritance (${...} substitutions).
    Config rawConfig =
        ConfigFactory.parseString(configContent).resolve(ConfigResolveOptions.defaults());

    // Convert the resolved Typesafe Config object into a standard Java Map for Jackson mapping
    return rawConfig.root().unwrapped();
  }
}
