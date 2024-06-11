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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper;

import com.google.cloud.teleport.v2.source.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.source.reader.io.exception.SuitableIndexNotFoundException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.TableConfig;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcSourceRowMapper;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaDiscovery;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaDiscoveryImpl;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo.IndexType;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.io.jdbc.JdbcIO.ReadWithPartitions;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcIoWrapper implements IoWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcIoWrapper.class);

  private final ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>>
      tableReaders;
  private final SourceSchema sourceSchema;

  private static final Logger logger = LoggerFactory.getLogger(JdbcIoWrapper.class);

  /**
   * Construct a JdbcIOWrapper from the configuration.
   *
   * @param config configuration for reading from a JDBC source.
   * @return JdbcIOWrapper
   * @throws SuitableIndexNotFoundException if a suitable index is not found to act as the partition
   *     column. Please refer to {@link JdbcIoWrapper#autoInferTableConfigs(JdbcIOWrapperConfig,
   *     SchemaDiscovery, DataSource)} for details on situation where this is thrown.
   */
  public static JdbcIoWrapper of(JdbcIOWrapperConfig config) throws SuitableIndexNotFoundException {
    DataSourceConfiguration dataSourceConfiguration = getDataSourceConfiguration(config);

    DataSource dataSource = dataSourceConfiguration.buildDatasource();

    SchemaDiscovery schemaDiscovery =
        new SchemaDiscoveryImpl(config.dialectAdapter(), config.schemaDiscoveryBackOff());

    ImmutableList<TableConfig> tableConfigs =
        autoInferTableConfigs(config, schemaDiscovery, dataSource);
    SourceSchema sourceSchema = getSourceSchema(config, schemaDiscovery, dataSource, tableConfigs);
    ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>> tableReaders =
        buildTableReaders(config, tableConfigs, dataSourceConfiguration, sourceSchema);
    return new JdbcIoWrapper(tableReaders, sourceSchema);
  }

  /**
   * Return a read transforms for the tables to migrate.
   *
   * @return Read transforms.
   */
  @Override
  public ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>>
      getTableReaders() {
    return this.tableReaders;
  }

  /**
   * Discover the schema of the source database.
   *
   * @return SourceSchema.
   */
  @Override
  public SourceSchema discoverTableSchema() {
    return this.sourceSchema;
  }

  static ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>>
      buildTableReaders(
          JdbcIOWrapperConfig config,
          ImmutableList<TableConfig> tableConfigs,
          DataSourceConfiguration dataSourceConfiguration,
          SourceSchema sourceSchema) {
    return tableConfigs.stream()
        .map(
            tableConfig -> {
              SourceTableSchema sourceTableSchema =
                  findSourceTableSchema(sourceSchema, tableConfig);
              return Map.entry(
                  SourceTableReference.builder()
                      .setSourceSchemaReference(sourceSchema.schemaReference())
                      .setSourceTableName(sourceTableSchema.tableName())
                      .setSourceTableSchemaUUID(sourceTableSchema.tableSchemaUUID())
                      .build(),
                  getJdbcIO(config, dataSourceConfiguration, tableConfig, sourceTableSchema));
            })
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  static SourceTableSchema findSourceTableSchema(
      SourceSchema sourceSchema, TableConfig tableConfig) {
    return sourceSchema.tableSchemas().stream()
        .filter(schema -> schema.tableName().equals(tableConfig.tableName()))
        .findFirst()
        .orElseThrow();
  }

  static SourceSchema getSourceSchema(
      JdbcIOWrapperConfig config,
      SchemaDiscovery schemaDiscovery,
      DataSource dataSource,
      ImmutableList<TableConfig> tableConfigs) {
    SourceSchema.Builder sourceSchemaBuilder =
        SourceSchema.builder().setSchemaReference(config.sourceSchemaReference());
    ImmutableList<String> tables =
        tableConfigs.stream().map(TableConfig::tableName).collect(ImmutableList.toImmutableList());
    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> tableSchemas =
        schemaDiscovery.discoverTableSchema(dataSource, config.sourceSchemaReference(), tables);
    LOG.info("Found table schemas: {}", tableSchemas);
    tableSchemas.entrySet().stream()
        .map(
            tableEntry -> {
              SourceTableSchema.Builder sourceTableSchemaBuilder =
                  SourceTableSchema.builder().setTableName(tableEntry.getKey());
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

  /**
   * Auto Infer the Partition Column and build table configuration. {@code autoInferTableConfigs}
   * discovers the list table index with the help of the passed {@code SchemaDisvovery}
   * implementation. Fom the list of indexes discovered, currently, it chooses a numeric primary key
   * column at the first ordinal position as PartitionColumn.
   *
   * @param config
   * @param schemaDiscovery
   * @param dataSource
   * @return
   */
  private static ImmutableList<TableConfig> autoInferTableConfigs(
      JdbcIOWrapperConfig config, SchemaDiscovery schemaDiscovery, DataSource dataSource) {
    ImmutableList<String> discoveredTables =
        schemaDiscovery.discoverTables(dataSource, config.sourceSchemaReference());
    ImmutableList<String> tables = getTablesToMigrate(config.tables(), discoveredTables);
    ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> indexes =
        schemaDiscovery.discoverTableIndexes(dataSource, config.sourceSchemaReference(), tables);
    ImmutableList.Builder<TableConfig> tableConfigsBuilder = ImmutableList.builder();
    for (String table : tables) {
      TableConfig.Builder configBuilder = TableConfig.builder(table);
      if (config.tableVsPartitionColumns().containsKey(table)) {
        config.tableVsPartitionColumns().get(table).forEach(configBuilder::withPartitionColum);
      } else {
        if (indexes.containsKey(table)) {
          // As of now only Primary key index with Numeric type is supported.
          // TODO:
          //    1. support unique indexes.
          //    2. support for DateTime type
          //    3. support for String type.
          ImmutableList<String> partitionColumns =
              indexes.get(table).stream()
                  .filter(
                      indexInfo ->
                          (indexInfo.isPrimary()
                              && indexInfo.ordinalPosition() == 1
                              && indexInfo.indexType() == IndexType.NUMERIC))
                  .map(SourceColumnIndexInfo::columnName)
                  .collect(ImmutableList.toImmutableList());
          if (partitionColumns.isEmpty()) {
            throw new SuitableIndexNotFoundException(
                new Throwable(
                    "No Suitable Index Found for partition column inference for table " + table));
          }
          partitionColumns.forEach(configBuilder::withPartitionColum);
        } else {
          throw new SuitableIndexNotFoundException(
              new Throwable("No Index Found for partition column inference for table " + table));
        }
      }
      if (config.maxPartitions() != null && config.maxPartitions() != 0) {
        configBuilder.setMaxPartitions(config.maxPartitions());
      }
      tableConfigsBuilder.add(configBuilder.build());
    }
    return tableConfigsBuilder.build();
  }

  @VisibleForTesting
  protected static ImmutableList<String> getTablesToMigrate(
      ImmutableList<String> configTables, ImmutableList<String> discoveredTables) {
    List<String> tables = null;
    if (configTables.isEmpty()) {
      tables = discoveredTables;
    } else {
      tables =
          configTables.stream()
              .filter(t -> discoveredTables.contains(t))
              .collect(Collectors.toList());
    }
    LOG.info("final list of tables to migrate: {}", tables);
    return ImmutableList.copyOf(tables);
  }

  /**
   * Private helper to construct {@link JdbcIO} as per the reader configuration.
   *
   * @param config Configuration.
   * @param dataSourceConfiguration dataSourceConfiguration (which is derived earlier from the
   *     reader configuration)
   * @param tableConfig discovered table configurations.
   * @param sourceTableSchema schema of the source table.
   * @return
   */
  private static PTransform<PBegin, PCollection<SourceRow>> getJdbcIO(
      JdbcIOWrapperConfig config,
      DataSourceConfiguration dataSourceConfiguration,
      TableConfig tableConfig,
      SourceTableSchema sourceTableSchema) {
    ReadWithPartitions<SourceRow, @UnknownKeyFor @NonNull @Initialized Long> jdbcIO =
        JdbcIO.<SourceRow>readWithPartitions()
            .withTable(tableConfig.tableName())
            .withPartitionColumn(tableConfig.partitionColumns().get(0))
            .withDataSourceProviderFn(JdbcIO.PoolableDataSourceProvider.of(dataSourceConfiguration))
            .withRowMapper(
                new JdbcSourceRowMapper(config.valueMappingsProvider(), sourceTableSchema));
    if (tableConfig.maxPartitions() != null) {
      jdbcIO = jdbcIO.withNumPartitions(tableConfig.maxPartitions());
    }
    return jdbcIO;
  }

  /**
   * Build the {@link DataSourceConfiguration} from the reader configuration.
   *
   * @param config reader configuration.
   * @return {@link DataSourceConfiguration}
   */
  private static DataSourceConfiguration getDataSourceConfiguration(JdbcIOWrapperConfig config) {

    DataSourceConfiguration dataSourceConfig =
        JdbcIO.DataSourceConfiguration.create(
                StaticValueProvider.of(config.jdbcDriverClassName()),
                StaticValueProvider.of(config.sourceDbURL()))
            .withMaxConnections(Math.toIntExact(config.maxConnections()));

    if (!config.sqlInitSeq().isEmpty()) {
      dataSourceConfig = dataSourceConfig.withConnectionInitSqls(config.sqlInitSeq());
    }

    if (config.jdbcDriverJars() != null && !config.jdbcDriverJars().isEmpty()) {
      dataSourceConfig = dataSourceConfig.withDriverJars(config.jdbcDriverJars());
    }
    if (!config.dbAuth().getUserName().get().isBlank()) {
      dataSourceConfig = dataSourceConfig.withUsername(config.dbAuth().getUserName().get());
    }
    if (!config.dbAuth().getPassword().get().isBlank()) {
      dataSourceConfig = dataSourceConfig.withPassword(config.dbAuth().getPassword().get());
    }

    LOG.info("Final DatasourceConfiguration: {}", dataSourceConfig);
    return dataSourceConfig;
  }

  /**
   * Private constructor for {@link JdbcIoWrapper}.
   *
   * @param tableReaders readers constructed from the reader configuration.
   * @param sourceSchema sourceSchema discoverd based on the reader configuration.
   *     <p>Note (implementation detail):
   *     <p>The external code should use the {@link JdbcIoWrapper#of} static method to construct the
   *     {@link JdbcIoWrapper} from the configuration. A private constructor and a public `of`
   *     method allows us to keep minimal logic in the constructor. This pattern is also followed by
   *     Beam classes like {@link JdbcIO}
   */
  private JdbcIoWrapper(
      ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>> tableReaders,
      SourceSchema sourceSchema) {
    this.tableReaders = tableReaders;
    this.sourceSchema = sourceSchema;
  }
}
