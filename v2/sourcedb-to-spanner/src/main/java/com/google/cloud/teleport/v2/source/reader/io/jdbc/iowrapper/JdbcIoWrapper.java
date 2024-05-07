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
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.TableConfig;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcSourceRowMapper;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaDiscovery;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaDiscoveryImpl;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
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
  private final ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>>
      tableReaders;
  private final SourceSchema sourceSchema;

  private static final Logger logger = LoggerFactory.getLogger(JdbcIoWrapper.class);

  public static JdbcIoWrapper of(JdbcIOWrapperConfig config) {
    DataSourceConfiguration dataSourceConfiguration = getDataSourceConfiguration(config);

    SourceSchema sourceSchema = getSourceSchema(config, dataSourceConfiguration);
    ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>> tableReaders =
        buildTableReaders(config, dataSourceConfiguration, sourceSchema);
    return new JdbcIoWrapper(tableReaders, sourceSchema);
  }

  @Override
  public ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>>
      getTableReaders() {
    return this.tableReaders;
  }

  @Override
  public SourceSchema discoverTableSchema() {
    return this.sourceSchema;
  }

  static ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>>
      buildTableReaders(
          JdbcIOWrapperConfig config,
          DataSourceConfiguration dataSourceConfiguration,
          SourceSchema sourceSchema) {
    return config.tableConfigs().stream()
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
      JdbcIOWrapperConfig config, DataSourceConfiguration dataSourceConfiguration) {
    SchemaDiscovery schemaDiscovery =
        new SchemaDiscoveryImpl(config.dialectAdapter(), config.schemaDiscoveryBackOff());
    SourceSchema.Builder sourceSchemaBuilder =
        SourceSchema.builder().setSchemaReference(config.sourceSchemaReference());
    DataSource dataSource = dataSourceConfiguration.buildDatasource();
    ImmutableList<String> tables =
        config.tableConfigs().stream()
            .map(TableConfig::tableName)
            .collect(ImmutableList.toImmutableList());
    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> tableSchemas =
        schemaDiscovery.discoverTableSchema(dataSource, config.sourceSchemaReference(), tables);
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
    if (tableConfig.maxFetchSize() != null) {
      jdbcIO = jdbcIO.withFetchSize(tableConfig.maxFetchSize());
    }
    if (tableConfig.maxPartitions() != null) {
      jdbcIO = jdbcIO.withNumPartitions(tableConfig.maxPartitions());
    }
    return jdbcIO;
  }

  private static DataSourceConfiguration getDataSourceConfiguration(JdbcIOWrapperConfig config) {

    DataSourceConfiguration dataSourceConfig =  JdbcIO.DataSourceConfiguration.create(
            StaticValueProvider.of(config.jdbcDriverClassName()),
            StaticValueProvider.of(getUrl(config)))
        .withDriverJars(config.jdbcDriverJars())
        .withMaxConnections(Math.toIntExact(config.maxConnections()));

    if (!config.dbAuth().getUserName().get().isBlank()) {
      dataSourceConfig = dataSourceConfig.withUsername(config.dbAuth().getUserName().get());
    }
    if (!config.dbAuth().getPassword().get().isBlank()) {
      dataSourceConfig = dataSourceConfig.withPassword(config.dbAuth().getPassword().get());
    }
    return  dataSourceConfig;
  }

  private static String getUrl(JdbcIOWrapperConfig config) {
    StringBuffer urlBuilder =
        new StringBuffer()
            .append(config.sourceHost())
            .append(":")
            .append(config.sourcePort())
            .append("/")
            .append(config.sourceSchemaReference().dbName());
    /* TODO: Handle PG Namespace */
    ImmutableList.Builder<String> attributesBuilder = new ImmutableList.Builder<>();
    if (config.autoReconnect()) {
      attributesBuilder
          .add("autoReconnect=true")
          .add("maxReconnects=" + config.reconnectAttempts());
    }
    String attributes = String.join("&", attributesBuilder.build());
    if (!attributes.isBlank()) {
      urlBuilder.append("?").append(attributes);
    }
    logger.debug("connection url is" + urlBuilder.toString());
    return urlBuilder.toString();
  }

  private JdbcIoWrapper(
      ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>> tableReaders,
      SourceSchema sourceSchema) {
    this.tableReaders = tableReaders;
    this.sourceSchema = sourceSchema;
  }
}
