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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.schema;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.CassandraConnector;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.CassandraDataSource;
import com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.schema.RetriableSchemaDiscovery;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.jline.utils.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CassandraSchemaDiscovery implements RetriableSchemaDiscovery {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraSchemaDiscovery.class);

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
  @Override
  public ImmutableList<String> discoverTables(
      DataSource dataSource, SourceSchemaReference sourceSchemaReference)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    Log.info(
        "CassandraSchemaDiscovery discoverTables started dataSource = {}, sourceSchemaReference = {}",
        dataSource,
        sourceSchemaReference);
    Preconditions.checkArgument(dataSource.getKind().equals(DataSource.Kind.CASSANDRA));
    Preconditions.checkArgument(
        sourceSchemaReference.getKind().equals(SourceSchemaReference.Kind.CASSANDRA));
    ImmutableList<String> tables =
        discoverTables(dataSource.cassandra(), sourceSchemaReference.cassandra());
    Log.info(
        "CassandraSchemaDiscovery discoverTables completed dataSource = {}, sourceSchemaReference = {}, tables = {}",
        dataSource,
        sourceSchemaReference,
        tables);
    return tables;
  }

  private ImmutableList<String> discoverTables(
      CassandraDataSource dataSource, CassandraSchemaReference sourceSchemaReference)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    try (CassandraConnector connector = new CassandraConnector(dataSource, sourceSchemaReference)) {
      return connector
          .getSession()
          .getMetadata()
          .getKeyspace(sourceSchemaReference.keyspaceName())
          .get()
          .getTables()
          .values()
          .stream()
          .map(RelationMetadata::getName)
          .map(n -> n.asCql(true))
          .collect(ImmutableList.toImmutableList());
    } catch (DriverException e) {
      Log.error(
          "CassandraSchemaDiscovery discoverTables dataSource = {}, sourceSchemaReference = {}",
          dataSource,
          sourceSchemaReference,
          e);
      throw new SchemaDiscoveryException(e);
    }
  }

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
  @Override
  public ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema(
      DataSource dataSource, SourceSchemaReference schemaReference, ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    Log.info(
        "CassandraSchemaDiscovery discoverTableSchema started dataSource = {}, sourceSchemaReference = {}, talbes = {}",
        dataSource,
        schemaReference,
        tables);
    Preconditions.checkArgument(dataSource.getKind().equals(DataSource.Kind.CASSANDRA));
    Preconditions.checkArgument(
        schemaReference.getKind().equals(SourceSchemaReference.Kind.CASSANDRA));
    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> schema =
        this.discoverTableSchema(dataSource.cassandra(), schemaReference.cassandra(), tables);
    Log.info(
        "CassandraSchemaDiscovery discoverTableSchema completed dataSource = {}, sourceSchemaReference = {}, talbes = {}, schema = {}",
        dataSource,
        schemaReference,
        tables,
        schema);
    return schema;
  }

  private ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema(
      CassandraDataSource dataSource,
      CassandraSchemaReference schemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    ImmutableMap.Builder<String, ImmutableMap<String, SourceColumnType>> builder =
        ImmutableMap.builder();
    try (CassandraConnector connector = new CassandraConnector(dataSource, schemaReference)) {
      KeyspaceMetadata keyspaceMetadata =
          connector.getSession().getMetadata().getKeyspace(schemaReference.keyspaceName()).get();
      for (String table : tables) {
        builder.put(table, getTableColumns(keyspaceMetadata, table));
      }
      return builder.build();
    } catch (DriverException e) {
      Log.error(
          "CassandraSchemaDiscovery discoverTableSchema dataSource = {}, sourceSchemaReference = {}, talbes = {}",
          dataSource,
          schemaReference,
          tables,
          e);
      throw new SchemaDiscoveryException(e);
    }
  }

  private ImmutableMap<String, SourceColumnType> getTableColumns(
      KeyspaceMetadata metadata, String table) {
    ImmutableMap.Builder<String, SourceColumnType> tableSchemaBuilder = ImmutableMap.builder();
    for (ColumnMetadata columnMetadata : metadata.getTable(table).get().getColumns().values()) {
      String name = columnMetadata.getName().toString();
      SourceColumnType sourceColumnType =
          new SourceColumnType(columnMetadata.getType().toString(), new Long[] {}, new Long[] {});
      tableSchemaBuilder.put(name, sourceColumnType);
    }
    return tableSchemaBuilder.build();
  }

  /**
   * Discover the indexes of tables to migrate. Note: As of now, we are using default CassandraIO
   * and don't see a need to override the partitioning logic. We don't need this part of schema
   * discovery for Cassandra and is an unsupported operation currently.
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
  @Override
  public ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> discoverTableIndexes(
      DataSource dataSource,
      SourceSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    Preconditions.checkArgument(dataSource.getKind().equals(DataSource.Kind.CASSANDRA));
    Preconditions.checkArgument(
        sourceSchemaReference.getKind().equals(SourceSchemaReference.Kind.CASSANDRA));
    return discoverTableIndexes(dataSource.cassandra(), sourceSchemaReference.cassandra(), tables);
  }

  private ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> discoverTableIndexes(
      CassandraDataSource dataSource,
      CassandraSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    throw new UnsupportedOperationException();
  }
}
