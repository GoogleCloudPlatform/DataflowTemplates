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

import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import javax.sql.DataSource;

/**
 * Discover Schema of Source Tables. The main Reader code must use the {@link SchemaDiscovery}
 * interface which internally handles retries as needed and throws only fatal exceptions.
 */
public interface SchemaDiscovery {

  /**
   * Discover the schema of tables to migrate.
   *
   * @param dataSource Provider for JDBC connection.
   * @param sourceSchemaReference Source database name and (optionally namespace)
   * @param tables Tables to migrate.
   * @return The discovered schema.
   * @throws SchemaDiscoveryException - Fatal exception during Schema Discovery.
   *     <p><b>Note:</b>
   *     <p>The Implementations must log every exception and generate metrics as appropriate. Any
   *     retriable error must be retried as needed.
   */
  ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema(
      DataSource dataSource,
      SourceSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException;
}
