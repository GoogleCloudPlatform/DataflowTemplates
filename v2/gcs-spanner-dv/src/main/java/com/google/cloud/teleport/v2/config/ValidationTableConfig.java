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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration class for table-based filtering in Data Validation pipeline.
 * Encapsulates parsing, matching, and validation of source and Spanner tables.
 */
public class ValidationTableConfig implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ValidationTableConfig.class);

  private final Set<String> configuredSourceTables;

  private ValidationTableConfig(Set<String> configuredSourceTables) {
    this.configuredSourceTables = configuredSourceTables;
  }

  /**
   * Creates an empty configuration with no filters. Useful for testing.
   */
  public static ValidationTableConfig empty() {
    return new ValidationTableConfig(new HashSet<>());
  }

  /**
   * Parses and validates table list from pipeline options.
   *
   * @param options The pipeline options.
   * @return A ValidationTableConfig instance containing the configured source tables.
   */
  public static ValidationTableConfig parseFromOptions(GCSSpannerDV.Options options) {
    String tablesConfig = options.getTables();
    String tableListFilePath = options.getTableListFilePath();
    boolean hasTablesConfig = tablesConfig != null && !tablesConfig.trim().isEmpty();
    boolean hasTableListFile = tableListFilePath != null && !tableListFilePath.trim().isEmpty();

    if (hasTablesConfig && hasTableListFile) {
      throw new IllegalArgumentException(
          "Both --tables and --tableListFilePath are provided. These options are mutually exclusive.");
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
        try (BufferedReader reader =
            new BufferedReader(
                Channels.newReader(FileSystems.open(resourceId), "UTF-8"))) {
          String line;
          while ((line = reader.readLine()) != null) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty()) {
              configuredTables.add(trimmed);
            }
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to read tableListFilePath: " + tableListFilePath, e);
      }
    }

    ValidationTableConfig config = new ValidationTableConfig(configuredTables);
    
    // Fail-Fast: dynamically verify that every explicitly requested table has matching files in GCS
    if (config.hasFilters()) {
      verifyTablesExistInGcs(configuredTables, options.getGcsInputDirectory());
    }

    return config;
  }

  /**
   * Helper function to fail fast if configured tables don't exist in GCS.
   * We use FileSystems.match(List<String>) to batch the requests efficiently.
   */
  private static void verifyTablesExistInGcs(Set<String> configuredTables, String gcsInputDirectory) {
    if (gcsInputDirectory == null || gcsInputDirectory.trim().isEmpty()) {
      return;
    }

    String cleanPath = gcsInputDirectory.endsWith("/") ? gcsInputDirectory : gcsInputDirectory + "/";
    java.util.List<String> tableList = new java.util.ArrayList<>(configuredTables);
    java.util.List<String> filePatterns = new java.util.ArrayList<>();
    
    for (String table : tableList) {
      filePatterns.add(cleanPath + table + "/**.avro");
    }

    try {
      java.util.List<org.apache.beam.sdk.io.fs.MatchResult> matchResults = FileSystems.match(filePatterns);
      java.util.List<String> missingTables = new java.util.ArrayList<>();

      for (int i = 0; i < matchResults.size(); i++) {
        org.apache.beam.sdk.io.fs.MatchResult result = matchResults.get(i);
        // A wildcard match that finds no files returns Status.OK but empty metadata
        if (result.status() != org.apache.beam.sdk.io.fs.MatchResult.Status.OK || result.metadata().isEmpty()) {
          missingTables.add(tableList.get(i));
        }
      }

      if (!missingTables.isEmpty()) {
        throw new IllegalArgumentException(
            "Fail-Fast GCS Verification: The following configured tables do not have matching .avro files in the source directory: " 
            + missingTables);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to verify table folders in GCS during initialization.", e);
    }
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
   * Checks if a Spanner table is allowed by the configuration.
   * Translates the Spanner table name to its source table counterpart using the schema mapper.
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
      LOG.warn("Could not map Spanner table '{}' back to a source table. Skipping validation.", spannerTableName);
      return false;
    }
  }
}
