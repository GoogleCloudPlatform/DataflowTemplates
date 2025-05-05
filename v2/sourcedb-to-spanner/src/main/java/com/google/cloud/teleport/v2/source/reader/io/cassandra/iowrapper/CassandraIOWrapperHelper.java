/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper;

import static com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.CassandraDefaults.DEFAULT_CASSANDRA_SCHEMA_DISCOVERY_BACKOFF;

import com.google.cloud.teleport.v2.source.reader.io.cassandra.schema.CassandraSchemaDiscovery;
import com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaDiscovery;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaDiscoveryImpl;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapper.MapperType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.FileNotFoundException;
import java.util.List;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Static Utility Class to provide basic functionality to {@link CassandraIoWrapper}. */
class CassandraIOWrapperHelper {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraIOWrapperHelper.class);

  static DataSource buildDataSource(String gcsPath, Integer numPartitions) {
    DataSource dataSource;
    try {
      dataSource =
          DataSource.ofCassandra(
              CassandraDataSource.builder()
                  .setOptionsMapFromGcsFile(gcsPath)
                  .setNumPartitions(numPartitions)
                  .build());
    } catch (FileNotFoundException e) {
      LOG.error("Unable to find driver config file in {}. Cause ", gcsPath, e);
      throw (new SchemaDiscoveryException(e));
    }
    return dataSource;
  }

  static SchemaDiscovery buildSchemaDiscovery() {
    return new SchemaDiscoveryImpl(
        new CassandraSchemaDiscovery(), DEFAULT_CASSANDRA_SCHEMA_DISCOVERY_BACKOFF);
  }

  static ImmutableList<String> getTablesToRead(
      List<String> sourceTables,
      DataSource dataSource,
      SchemaDiscovery schemaDiscovery,
      SourceSchemaReference sourceSchemaReference) {
    ImmutableList<String> tablesToRead;
    if (sourceTables.isEmpty()) {
      tablesToRead = schemaDiscovery.discoverTables(dataSource, sourceSchemaReference);
      LOG.info("Auto Discovered SourceTables = {}, Tables = {}", sourceTables, tablesToRead);
    } else {
      ImmutableSet<String> existingTables =
          schemaDiscovery.discoverTables(dataSource, sourceSchemaReference).stream()
              .collect(ImmutableSet.toImmutableSet());
      tablesToRead =
          sourceTables.stream()
              .filter(t -> existingTables.contains(t))
              .collect(ImmutableList.toImmutableList());
      LOG.info(
          "Using tables from passed SourceTables = {}, existing tables = {}, tables to read = {}. Tables not present on source shall be ignored",
          sourceTables,
          existingTables,
          tablesToRead);
    }
    return tablesToRead;
  }

  static SourceSchema getSourceSchema(
      SchemaDiscovery schemaDiscovery,
      DataSource dataSource,
      SourceSchemaReference sourceSchemaReference,
      ImmutableList<String> tables) {

    SourceSchema.Builder sourceSchemaBuilder =
        SourceSchema.builder().setSchemaReference(sourceSchemaReference);
    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> tableSchemas =
        schemaDiscovery.discoverTableSchema(dataSource, sourceSchemaReference, tables);
    LOG.info("Found table schemas: {}", tableSchemas);
    tableSchemas.entrySet().stream()
        .map(
            tableEntry -> {
              SourceTableSchema.Builder sourceTableSchemaBuilder =
                  SourceTableSchema.builder(MapperType.CASSANDRA).setTableName(tableEntry.getKey());
              tableEntry
                  .getValue()
                  .entrySet()
                  .forEach(
                      colEntry ->
                          sourceTableSchemaBuilder.addSourceColumnNameToSourceColumnType(
                              colEntry.getKey(), colEntry.getValue()));
              return sourceTableSchemaBuilder.build();
            })
        .forEach(sourceSchemaBuilder::addTableSchema);
    return sourceSchemaBuilder.build();
  }

  static ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>>
      getTableReaders(DataSource dataSource, SourceSchema sourceSchema) {
    /*
     * TODO(vardhanvthigle): Plugin alternate implementation if needed.
     */
    CassandraTableReaderFactory cassandraTableReaderFactory =
        new CassandraTableReaderFactoryCassandraIoImpl();
    ImmutableMap.Builder<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>>
        tableReadersBuilder = ImmutableMap.builder();
    SourceSchemaReference sourceSchemaReference = sourceSchema.schemaReference();
    sourceSchema
        .tableSchemas()
        .forEach(
            tableSchema -> {
              SourceTableReference sourceTableReference =
                  SourceTableReference.builder()
                      .setSourceSchemaReference(sourceSchemaReference)
                      .setSourceTableSchemaUUID(tableSchema.tableSchemaUUID())
                      .setSourceTableName(tableSchema.tableName())
                      .build();
              var tableReader =
                  cassandraTableReaderFactory.getTableReader(
                      dataSource.cassandra(), sourceSchemaReference, tableSchema);
              tableReadersBuilder.put(sourceTableReference, tableReader);
            });
    return tableReadersBuilder.build();
  }

  private CassandraIOWrapperHelper() {}
}
