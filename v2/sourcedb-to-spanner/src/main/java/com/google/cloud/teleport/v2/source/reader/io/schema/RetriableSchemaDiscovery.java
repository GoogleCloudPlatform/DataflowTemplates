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
package com.google.cloud.teleport.v2.source.reader.io.schema;

import com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Discover the schema of tables to migrate. Implementation must adapt to requirements of the source
 * database type and the dialect as needed. For any errors that should be retried, the
 * implementation must return {@link RetriableSchemaDiscoveryException}.
 *
 * <p><b>Note:</b> The main Reader code must use the {@link SchemaDiscovery} interface which
 * internally handles retries as needed and throws only fatal exceptions. Various Sources and their
 * dialects can implement the {@link RetriableSchemaDiscovery} interface. The {@link
 * SchemaDiscoveryImpl} will wrap the {@link RetriableSchemaDiscovery} interface and do the retries
 * as needed.
 */
public interface RetriableSchemaDiscovery {

  /**
   * Discover Tables to migrate. This method could be used to auto infer tables to migrate if not
   * passed via options.
   *
   * @param dataSource Provider for source connection.
   * @param sourceSchemaReference Source database name and (optionally namespace)
   * @return The list of table names for the given database.
   * @throws SchemaDiscoveryException - Fatal exception during Schema Discovery.
   * @throws RetriableSchemaDiscoveryException - Retriable exception during Schema Discovery.
   *     <p><b>Note:</b>
   *     <p>The Implementations must log every exception and generate metrics as appropriate.
   */
  ImmutableList<String> discoverTables(
      DataSource dataSource, SourceSchemaReference sourceSchemaReference)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException;

  /**
   * Discover the schema of tables to migrate.
   *
   * @param dataSource Provider for source connection.
   * @param schemaReference Source database name and (optionally namespace)
   * @param tables Tables to migrate.
   * @return The discovered schema.
   * @throws SchemaDiscoveryException - Fatal exception during Schema Discovery.
   * @throws RetriableSchemaDiscoveryException - Retriable exception during Schema Discovery.
   *     <p><b>Note:</b>
   *     <p>The Implementations must log every exception and generate metrics as appropriate.
   */
  ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema(
      DataSource dataSource, SourceSchemaReference schemaReference, ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException;

  /**
   * Discover the indexes of tables to migrate.
   *
   * @param dataSource Provider for source connection.
   * @param sourceSchemaReference Source database name and (optionally namespace)
   * @param tables Tables to migrate.
   * @return The discovered indexes.
   * @throws SchemaDiscoveryException - Fatal exception during Schema Discovery.
   * @throws RetriableSchemaDiscoveryException - Retriable exception during Schema Discovery.
   *     <p><b>Note:</b>
   *     <p>The Implementations must log every exception and generate metrics as appropriate.
   */
  ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> discoverTableIndexes(
      DataSource dataSource,
      SourceSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException;
}
