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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CassandraDriverConfigLoader;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ISecretManagerAccessor;
import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigResolveOptions;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses the source configuration from JSON/HOCON and resolves secrets from Secret Manager.
 *
 * <p>Supports the following source types: MySQL, PostgreSQL, Cassandra, and Astra DB.
 */
public class SourceConfigParser {
  private static final Logger LOG = LoggerFactory.getLogger(SourceConfigParser.class);
  private static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private final ISecretManagerAccessor secretManagerAccessor;

  /**
   * Constructs a new {@code SourceConfigParser} with the specified secret manager accessor.
   *
   * @param secretManagerAccessor the accessor used to resolve secrets from Secret Manager
   */
  public SourceConfigParser(ISecretManagerAccessor secretManagerAccessor) {
    this.secretManagerAccessor = secretManagerAccessor;
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

    SourceType sourceType = SourceType.parseSourceType(sourceTypeStr);
    switch (SourceType.parseSourceType(sourceTypeStr)) {
      case CASSANDRA:
        // Maps directly to the DataStax OptionsMap
        return new CassandraConnectionConfig(
            CassandraDriverConfigLoader.getOptionsMapFromFile(sourceConfigFilePath));
      case ASTRA_DB:
        String astraFileContent = readConfigFilePath(sourceConfigFilePath);
        Map<String, Object> astraConfigMap = parseConfigToConfigMap(astraFileContent);
        return mapper.convertValue(astraConfigMap, AstraConnectionConfig.class);
      case MYSQL:
      case PG:
        String jdbcFileContent = readConfigFilePath(sourceConfigFilePath);
        Map<String, Object> jdbcConfigMap = parseConfigToConfigMap(jdbcFileContent);
        JdbcShardConfig jdbcShardConfig = mapper.convertValue(jdbcConfigMap, JdbcShardConfig.class);
        resolveShardSecret(jdbcShardConfig, sourceConfigFilePath);
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
    for (Shard shard : jdbcShardConfig.getShardConfigs()) {
      LOG.info(" The shard is: {} ", shard);
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

  /**
   * Reads the content of a configuration file from a specified file path (e.g., GCS or local).
   *
   * @param sourceConfigFilePath the path or URI to the configuration file
   * @return the content of the file as a string
   * @throws Exception if an error occurs while reading the file
   */
  @VisibleForTesting
  static String readConfigFilePath(String sourceConfigFilePath) throws Exception {
    try (InputStream stream =
        Channels.newInputStream(
            FileSystems.open(FileSystems.matchNewResource(sourceConfigFilePath, false)))) {

      return IOUtils.toString(stream, StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOG.error(
          "Failed to read configuration input file. Make sure it is ASCII or UTF-8 encoded and contains a"
              + " well-formed JSON string.",
          e);
      throw new RuntimeException(
          "Failed to read configuration input file. Make sure it is ASCII or UTF-8 encoded and contains a"
              + " well-formed JSON string.",
          e);
    }
  }
}
