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
package com.google.cloud.teleport.v2.config;

import com.google.cloud.teleport.v2.options.GCSSpannerDVOptions;
import com.google.gson.Gson;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.commons.io.IOUtils;

/**
 * Configuration class for the Data Validation pipeline: table-based filtering, the selected shard
 * IDs, and per-table configuration (e.g. {@code spannerQuery}). Encapsulates parsing, matching, and
 * validation of source and Spanner tables.
 */
public class TableConfiguration implements Serializable {

  private final Set<String> configuredSourceTables;

  /** Logical shard IDs from {@code --shardIds}: trimmed and de-duplicated. */
  private final Set<String> shardIds;

  /**
   * Source table name (config key, as written) to its per-table configuration. Only tables with a
   * non-blank {@code spannerQuery} are present.
   */
  private final Map<String, TableLevelConfig> tableLevelConfigs;

  private TableConfiguration(
      Set<String> configuredSourceTables,
      Set<String> shardIds,
      Map<String, TableLevelConfig> tableLevelConfigs) {
    this.configuredSourceTables = Collections.unmodifiableSet(configuredSourceTables);
    this.shardIds = Collections.unmodifiableSet(new HashSet<>(shardIds));
    this.tableLevelConfigs = Collections.unmodifiableMap(new LinkedHashMap<>(tableLevelConfigs));
  }

  /** Creates an empty configuration with no filters. Useful for testing. */
  public static TableConfiguration empty() {
    return new TableConfiguration(new HashSet<>(), Collections.emptySet(), new LinkedHashMap<>());
  }

  /**
   * Parses and validates table configuration from pipeline options.
   *
   * @param options The pipeline options.
   * @return A TableConfiguration instance containing the configured source tables, the selected
   *     shard IDs, and the per-table configuration (e.g. {@code spannerQuery}).
   */
  public static TableConfiguration parseFromOptions(GCSSpannerDVOptions options) {
    String tablesConfig = options.getTables();
    String tableConfigurationFilePath = options.getTableConfigurationFilePath();
    boolean hasTablesConfig = tablesConfig != null && !tablesConfig.trim().isEmpty();
    boolean hasTableConfigFile =
        tableConfigurationFilePath != null && !tableConfigurationFilePath.trim().isEmpty();

    if (hasTablesConfig && hasTableConfigFile) {
      throw new IllegalArgumentException(
          "Both --tables and --tableConfigurationFilePath are provided. Please configure only one of these parameters at a time.");
    }

    Set<String> configuredTables = new HashSet<>();
    Map<String, TableLevelConfig> tableLevelConfigs = new LinkedHashMap<>();

    if (hasTablesConfig) {
      configuredTables = parseCommaSeparatedNames(tablesConfig);
    } else if (hasTableConfigFile) {
      List<String> fileTableNames = Collections.emptyList();
      try {
        ResourceId resourceId = FileSystems.matchNewResource(tableConfigurationFilePath, false);
        try (InputStream stream = Channels.newInputStream(FileSystems.open(resourceId))) {
          String result = IOUtils.toString(stream, StandardCharsets.UTF_8);
          Gson gson = new Gson();
          TableConfigurationFile fileConfig = gson.fromJson(result, TableConfigurationFile.class);

          if (fileConfig != null && fileConfig.getTableNames() != null) {
            fileTableNames = fileConfig.getTableNames();
          }

          if (fileConfig != null && fileConfig.getOptionalConfigurations() != null) {
            for (Map.Entry<String, TableLevelConfig> entry :
                fileConfig.getOptionalConfigurations().entrySet()) {
              // A null entry or a null, absent or blank spannerQuery means "not configured".
              if (entry.getValue() == null
                  || entry.getValue().getSpannerQuery() == null
                  || entry.getValue().getSpannerQuery().trim().isEmpty()) {
                continue;
              }
              tableLevelConfigs.put(entry.getKey(), entry.getValue());
            }
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to read JSON tableConfigurationFilePath: " + tableConfigurationFilePath, e);
      }
      configuredTables = parseNames(fileTableNames);
    }

    Set<String> shardIds = parseCommaSeparatedNames(options.getShardIds());
    return new TableConfiguration(configuredTables, shardIds, tableLevelConfigs);
  }

  /** Splits a comma-separated option (e.g. {@code --tables}, {@code --shardIds}) into values. */
  private static Set<String> parseCommaSeparatedNames(String commaSeparated) {
    return commaSeparated == null
        ? new HashSet<>()
        : parseNames(Arrays.asList(commaSeparated.split(",")));
  }

  /** Trims each value, skips empty ones and de-duplicates. */
  private static Set<String> parseNames(Iterable<String> values) {
    Set<String> result = new HashSet<>();
    for (String value : values) {
      String trimmed = value.trim();
      if (!trimmed.isEmpty()) {
        result.add(trimmed);
      }
    }
    return result;
  }

  public boolean hasTableFilters() {
    return configuredSourceTables != null && !configuredSourceTables.isEmpty();
  }

  public Set<String> getSourceTables() {
    return configuredSourceTables;
  }

  /** Returns true iff {@code --shardIds} selects at least one shard. */
  public boolean hasShardFilter() {
    return !shardIds.isEmpty();
  }

  /**
   * Returns the selected logical shard IDs: unmodifiable, trimmed and de-duplicated. Empty when
   * shard subsetting is off.
   */
  public Set<String> getShardIds() {
    return shardIds;
  }

  /** Returns true iff at least one table has a {@code spannerQuery} configured. */
  public boolean hasSpannerQueries() {
    return !tableLevelConfigs.isEmpty();
  }

  /**
   * Returns an unmodifiable map of source table name (config key, as written) to the raw {@code
   * spannerQuery} text. Never null.
   */
  public Map<String, String> getSpannerQueries() {
    Map<String, String> spannerQueries = new LinkedHashMap<>();
    for (Map.Entry<String, TableLevelConfig> entry : tableLevelConfigs.entrySet()) {
      spannerQueries.put(entry.getKey(), entry.getValue().getSpannerQuery());
    }
    return Collections.unmodifiableMap(spannerQueries);
  }

  /**
   * Checks if a source table is allowed by the configuration.
   *
   * @param sourceTableName The source table name.
   * @return true if allowed or no filters are configured, false otherwise.
   */
  public boolean isSourceTableAllowed(String sourceTableName) {
    if (!hasTableFilters()) {
      return true;
    }
    return configuredSourceTables.contains(sourceTableName);
  }
}
