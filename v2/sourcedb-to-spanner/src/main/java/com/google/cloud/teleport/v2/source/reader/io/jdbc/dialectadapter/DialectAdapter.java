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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter;

import com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.source.reader.io.schema.RetriableSchemaDiscovery;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference.Kind;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Interface to support various dialects of JDBC databases.
 *
 * <p><b>Note:</b>As a prt of M2 effort, this interface will expose more mehtods than just extending
 * {@link RetriableSchemaDiscovery}.
 */
public interface DialectAdapter extends RetriableSchemaDiscovery, UniformSplitterDBAdapter {

  /**
   * Discover Tables to migrate. This method could be used to auto infer tables to migrate if not
   * passed via options.
   *
   * @param dataSource Provider for JDBC connection.
   * @param sourceSchemaReference Source database name and (optionally namespace)
   * @return The list of table names for the given database.
   * @throws SchemaDiscoveryException - Fatal exception during Schema Discovery.
   * @throws RetriableSchemaDiscoveryException - Retriable exception during Schema Discovery.
   *     <p><b>Note:</b>
   *     <p>The Implementations must log every exception and generate metrics as appropriate.
   */
  default ImmutableList<String> discoverTables(
      DataSource dataSource, SourceSchemaReference sourceSchemaReference)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    Preconditions.checkArgument(sourceSchemaReference.getKind() == Kind.JDBC);
    return discoverTables(dataSource.jdbc(), sourceSchemaReference.jdbc());
  }

  ImmutableList<String> discoverTables(
      javax.sql.DataSource dataSource, JdbcSchemaReference sourceSchemaReference)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException;

  /**
   * Discover the schema of tables to migrate.
   *
   * @param dataSource Provider for JDBC connection.
   * @param schemaReference Source database name and (optionally namespace)
   * @param tables Tables to migrate.
   * @return The discovered schema.
   * @throws SchemaDiscoveryException - Fatal exception during Schema Discovery.
   * @throws RetriableSchemaDiscoveryException - Retriable exception during Schema Discovery.
   *     <p><b>Note:</b>
   *     <p>The Implementations must log every exception and generate metrics as appropriate.
   */
  default ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema(
      DataSource dataSource, SourceSchemaReference schemaReference, ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    Preconditions.checkArgument(schemaReference.getKind() == Kind.JDBC);
    return discoverTableSchema(dataSource.jdbc(), schemaReference.jdbc(), tables);
  }

  ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema(
      javax.sql.DataSource dataSource,
      JdbcSchemaReference schemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException;

  /**
   * Discover the indexes of tables to migrate.
   *
   * @param dataSource Provider for JDBC connection.
   * @param sourceSchemaReference Source database name and (optionally namespace)
   * @param tables Tables to migrate.
   * @return The discovered indexes.
   * @throws SchemaDiscoveryException - Fatal exception during Schema Discovery.
   * @throws RetriableSchemaDiscoveryException - Retriable exception during Schema Discovery.
   *     <p><b>Note:</b>
   *     <p>The Implementations must log every exception and generate metrics as appropriate.
   */
  default ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> discoverTableIndexes(
      DataSource dataSource,
      SourceSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    Preconditions.checkArgument(sourceSchemaReference.getKind() == Kind.JDBC);
    return discoverTableIndexes(dataSource.jdbc(), sourceSchemaReference.jdbc(), tables);
  }

  ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> discoverTableIndexes(
      javax.sql.DataSource dataSource,
      JdbcSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException;
}
