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

import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.templates.GCSSpannerDV;
import com.google.gson.Gson;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration class for table-based filtering in Data Validation pipeline. Encapsulates parsing,
 * matching, and validation of source and Spanner tables.
 */
public class TableConfiguration implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(TableConfiguration.class);

  private final Set<String> configuredSourceTables;

  private TableConfiguration(Set<String> configuredSourceTables) {
    this.configuredSourceTables = configuredSourceTables;
  }

  /** Creates an empty configuration with no filters. Useful for testing. */
  public static TableConfiguration empty() {
    return new TableConfiguration(new HashSet<>());
  }

  /**
   * Parses and validates table list from pipeline options.
   *
   * @param options The pipeline options.
   * @return A TableConfiguration instance containing the configured source tables.
   */
  public static TableConfiguration parseFromOptions(GCSSpannerDV.Options options) {
    String tablesConfig = options.getTables();
    String tableListFilePath = options.getTableListFilePath();
    boolean hasTablesConfig = tablesConfig != null && !tablesConfig.trim().isEmpty();
    boolean hasTableListFile = tableListFilePath != null && !tableListFilePath.trim().isEmpty();

    if (hasTablesConfig && hasTableListFile) {
      throw new IllegalArgumentException(
          "Both --tables and --tableListFilePath are provided. Please configure only one of these parameters at a time.");
    }

    Set<String> configuredTables = new HashSet<>();

    if (hasTablesConfig) {
      for (String table : tablesConfig.split(",")) {
        String trimmed = table.trim();
        if (!trimmed.isEmpty()) {
          configuredTables.add(trimmed);
        }
      }
    } else if (hasTableListFile) {
      try {
        ResourceId resourceId = FileSystems.matchNewResource(tableListFilePath, false);
        try (InputStream stream = Channels.newInputStream(FileSystems.open(resourceId))) {
          String result = IOUtils.toString(stream, StandardCharsets.UTF_8);
          Gson gson = new Gson();
          TableListConfig fileConfig = gson.fromJson(result, TableListConfig.class);

          if (fileConfig != null && fileConfig.getTableNames() != null) {
            for (String table : fileConfig.getTableNames()) {
              String trimmed = table.trim();
              if (!trimmed.isEmpty()) {
                configuredTables.add(trimmed);
              }
            }
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to read JSON tableListFilePath: " + tableListFilePath, e);
      }
    }

    TableConfiguration config = new TableConfiguration(configuredTables);

    return config;
  }

  public boolean hasFilters() {
    return configuredSourceTables != null && !configuredSourceTables.isEmpty();
  }

  public Set<String> getSourceTables() {
    return configuredSourceTables;
  }

  /**
   * Checks if a source table is allowed by the configuration.
   *
   * @param sourceTableName The source table name.
   * @return true if allowed or no filters are configured, false otherwise.
   */
  public boolean isSourceTableAllowed(String sourceTableName) {
    if (!hasFilters()) {
      return true;
    }
    return configuredSourceTables.contains(sourceTableName);
  }

  /**
   * Checks if a Spanner table is allowed by the configuration. Translates the Spanner table name to
   * its source table counterpart using the schema mapper.
   *
   * @param spannerTableName The Spanner table name.
   * @param schemaMapper The schema mapper to translate the table name.
   * @return true if allowed or no filters are configured, false otherwise.
   */
  public boolean isSpannerTableAllowed(String spannerTableName, ISchemaMapper schemaMapper) {
    if (!hasFilters()) {
      return true;
    }
    try {
      String sourceTable = schemaMapper.getSourceTableName("", spannerTableName);
      return configuredSourceTables.contains(sourceTable);
    } catch (NoSuchElementException e) {
      LOG.warn(
          "Could not map Spanner table '{}' back to a source table. Skipping validation.",
          spannerTableName);
      return false;
    }
  }
}
