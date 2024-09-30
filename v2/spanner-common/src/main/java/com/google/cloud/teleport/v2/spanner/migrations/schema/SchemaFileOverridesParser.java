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
package com.google.cloud.teleport.v2.spanner.migrations.schema;

import com.google.gson.Gson;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaFileOverridesParser implements ISchemaOverridesParser, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaFileOverridesParser.class);
  private static Gson gson = new Gson();

  final SchemaFileOverride schemaFileOverride;

  public SchemaFileOverridesParser(String overridesFilePath) {
    try (InputStream stream =
        Channels.newInputStream(
            FileSystems.open(FileSystems.matchNewResource(overridesFilePath, false)))) {
      String result = IOUtils.toString(stream, StandardCharsets.UTF_8);
      schemaFileOverride = gson.fromJson(result, SchemaFileOverride.class);
      LOG.info("schemaFileOverride = " + schemaFileOverride.toString());
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to read schema overrides file. Make sure it is ASCII or UTF-8 encoded and contains a"
              + " well-formed JSON string.",
          e);
    }
  }

  /**
   * Gets the spanner table name given the source table name, or source table name if no override is
   * configured.
   *
   * @param sourceTableName The source table name
   * @return The overridden spanner table name
   */
  @Override
  public String getTableOverride(String sourceTableName) {
    if (schemaFileOverride.getRenamedTables() == null) {
      return sourceTableName;
    }
    return schemaFileOverride.getRenamedTables().getOrDefault(sourceTableName, sourceTableName);
  }

  /**
   * Gets the spanner column name given the source table name, or the source column name if override
   * is configured.
   *
   * @param sourceTableName the source table name for which column name is overridden
   * @param sourceColumnName the source column name being overridden
   * @return A pair of spannerTableName and spannerColumnName
   */
  @Override
  public Pair<String, String> getColumnOverride(String sourceTableName, String sourceColumnName) {
    if (schemaFileOverride.getRenamedColumnTupleMap() == null
        || schemaFileOverride.getRenamedColumnTupleMap().get(sourceTableName) == null) {
      return new ImmutablePair<>(sourceTableName, sourceColumnName);
    }
    Map<String, String> tableOverridesMap =
        schemaFileOverride.getRenamedColumnTupleMap().get(sourceTableName);
    return new ImmutablePair<>(
        sourceTableName, tableOverridesMap.getOrDefault(sourceColumnName, sourceColumnName));
  }
}
