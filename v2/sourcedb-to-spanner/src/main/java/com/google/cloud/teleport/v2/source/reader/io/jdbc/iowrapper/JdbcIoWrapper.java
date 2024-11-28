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

import static com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo.INDEX_TYPE_TO_CLASS;

import com.google.cloud.teleport.v2.source.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource;
import com.google.cloud.teleport.v2.source.reader.io.exception.SuitableIndexNotFoundException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.TableConfig;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcSourceRowMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms.ReadWithUniformPartitions;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaDiscovery;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaDiscoveryImpl;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo.IndexType;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.io.jdbc.JdbcIO.ReadWithPartitions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.dbcp2.BasicDataSource;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * TODO(vardhanvthigle): Towards M3, make this a reconfigurable class, and expose (if required) the approxRowCounts
 *  and maxPartitionHints (auto inferred) to the pipeline controller helping a better sequencing of tables.
 */
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

    javax.sql.DataSource dataSource = dataSourceConfiguration.buildDatasource();
    setDataSourceLoginTimeout((BasicDataSource) dataSource, config);

    SchemaDiscovery schemaDiscovery =
        new SchemaDiscoveryImpl(config.dialectAdapter(), config.schemaDiscoveryBackOff());

    ImmutableList<TableConfig> tableConfigs =
        autoInferTableConfigs(config, schemaDiscovery, DataSource.ofJdbc(dataSource));
    SourceSchema sourceSchema =
        getSourceSchema(config, schemaDiscovery, DataSource.ofJdbc(dataSource), tableConfigs);
    ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>> tableReaders =
        buildTableReaders(config, tableConfigs, dataSourceConfiguration, sourceSchema);
    return new JdbcIoWrapper(tableReaders, sourceSchema);
  }

  /**
   * Set's the login timeout for the DataSource used for schema and index discoveries. This helps in
   * early error reporting to the customer in case of unreachable or unavailable source database.
   * The default login timeout for the {@link BasicDataSource} is infinite. Unfortunately, {@link
   * BasicDataSource} does not directly support {@link DataSource#setLoginTimeout(int)}. This can be
   * achieved by setting {@link BasicDataSource#setMaxWaitMillis} and connect timeout at the driver
   * layer.
   *
   * @param dataSource
   * @param config
   */
  @VisibleForTesting
  protected static void setDataSourceLoginTimeout(
      BasicDataSource dataSource, JdbcIOWrapperConfig config) {

    dataSource.setMaxWaitMillis(config.schemaDiscoveryConnectivityTimeoutMilliSeconds());

    String connectivityTimeout;
    switch (config.sourceDbDialect()) {
      case MYSQL:
        connectivityTimeout =
            String.valueOf(config.schemaDiscoveryConnectivityTimeoutMilliSeconds());
        setConnectionProperty(dataSource, "connectTimeout", connectivityTimeout);
        setConnectionProperty(dataSource, "socketTimeout", connectivityTimeout);
        break;
      case POSTGRESQL:
        connectivityTimeout =
            String.valueOf(config.schemaDiscoveryConnectivityTimeoutMilliSeconds() / 1000);
        setConnectionProperty(dataSource, "loginTimeout", connectivityTimeout);
        setConnectionProperty(dataSource, "connectTimeout", connectivityTimeout);
        setConnectionProperty(dataSource, "socketTimeout", connectivityTimeout);
        break;
      default:
        logger.error(
            "No connectivity timeout overrides implemented for dialect {}. In case of misconfigured network connectivity, schema discovery could timeout without correct error reporting.");
    }
  }

  private static void setConnectionProperty(
      BasicDataSource dataSource, String property, String value) {

    String url = dataSource.getUrl();
    if (!url.contains(property)) {
      dataSource.addConnectionProperty(property, value);
      logger.info("Set {} = {}  for schema discovery of {}", property, value, dataSource);
    } else {
      logger.warn(
          "Property {} already set in URL {}. Not overriding with {} for schema discovery. The default over-ride helps in failing fast in case of misconfigured network connectivity.",
          property,
          url,
          value);
    }
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
                      .setSourceTableName(delimitIdentifier(sourceTableSchema.tableName()))
                      .setSourceTableSchemaUUID(sourceTableSchema.tableSchemaUUID())
                      .build(),
                  (config.readWithUniformPartitionsFeatureEnabled())
                      ? getReadWithUniformPartitionIO(
                          config,
                          dataSourceConfiguration,
                          sourceSchema.schemaReference(),
                          tableConfig,
                          sourceTableSchema)
                      : getJdbcIO(
                          config,
                          dataSourceConfiguration,
                          sourceSchema.schemaReference(),
                          tableConfig,
                          sourceTableSchema));
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
                  SourceTableSchema.builder(config.sourceDbDialect())
                      .setTableName(tableEntry.getKey());
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
    if (tables.isEmpty()) {
      logger.info("source does not contain matching tables: {}", config.tables());
      return ImmutableList.of();
    }
    ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> indexes =
        schemaDiscovery.discoverTableIndexes(dataSource, config.sourceSchemaReference(), tables);
    ImmutableList.Builder<TableConfig> tableConfigsBuilder = ImmutableList.builder();
    for (String table : tables) {
      tableConfigsBuilder.add(getTableConfig(table, config, indexes));
    }
    return tableConfigsBuilder.build();
  }

  private static TableConfig getTableConfig(
      String tableName,
      JdbcIOWrapperConfig config,
      ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> indexInfo) {
    TableConfig.Builder tableConfigBuilder = TableConfig.builder(tableName);
    if (config.maxPartitions() != null && config.maxPartitions() != 0) {
      tableConfigBuilder.setMaxPartitions(config.maxPartitions());
    }
    /*
     * TODO(vardhanvthigle): Add optional support for non-primary indexes.
     * Note: most of the implementation is generic for any unique index.
     *  Need to benchmark and do the end to end implementation.
     */
    if (indexInfo.containsKey(tableName)) {
      ImmutableList<SourceColumnIndexInfo> tableIndexInfo = indexInfo.get(tableName);

      // TODO(vardhanvthigle): support for non-primary indexes.
      tableIndexInfo.stream()
          .filter(info -> info.isPrimary() && info.ordinalPosition() == 1)
          .map(SourceColumnIndexInfo::cardinality)
          .forEach(tableConfigBuilder::setApproxRowCount);
      if (config.tableVsPartitionColumns().containsKey(tableName)) {
        config.tableVsPartitionColumns().get(tableName).stream()
            .map(
                colName ->
                    tableIndexInfo.stream()
                        .filter(info -> info.columnName().equals(colName))
                        .findFirst()
                        .get())
            .sorted()
            .map(JdbcIoWrapper::partitionColumnFromIndexInfo)
            .forEach(tableConfigBuilder::withPartitionColum);
      } else {
        ImmutableSet<IndexType> supportedIndexTypes =
            ImmutableSet.of(IndexType.NUMERIC, IndexType.STRING, IndexType.BIG_INT_UNSIGNED);
        // As of now only Primary key index with Numeric type is supported.
        // TODO:
        //    1. support non-primary unique indexes.
        //        Note: most of the implementation is generic for any unique index.
        //        Need to benchmark and do the end to end implementation.
        //    2. support for DateTime type
        //    3. support for composite indexes
        //       Note: though we have most of the code for composite index, since we cap the
        // splitting stages to 1, additional indexes will not be considered for splitting as of now.
        tableIndexInfo.stream()
            .filter(
                idxInfo ->
                    (idxInfo.isPrimary() && supportedIndexTypes.contains(idxInfo.indexType())))
            .sorted()
            .map(JdbcIoWrapper::partitionColumnFromIndexInfo)
            .forEach(tableConfigBuilder::withPartitionColum);
      }
      TableConfig tableConfig = tableConfigBuilder.build();
      if (tableConfig.partitionColumns().isEmpty()) {
        throw new SuitableIndexNotFoundException(
            new Throwable(
                "No Suitable Index Found for partition column inference for table " + tableName));
      }
      return tableConfig;
    } else {
      throw new SuitableIndexNotFoundException(
          new Throwable("No Index Found for partition column inference for table " + tableName));
    }
  }

  @VisibleForTesting
  protected static java.lang.Class indexTypeToColumnClass(SourceColumnIndexInfo indexInfo)
      throws SuitableIndexNotFoundException {
    if (INDEX_TYPE_TO_CLASS.containsKey(indexInfo.indexType())) {
      return INDEX_TYPE_TO_CLASS.get(indexInfo.indexType());
    } else {
      throw new SuitableIndexNotFoundException(
          new Throwable("No class Mapping for IndexType " + indexInfo));
    }
  }

  /**
   * Delimit the Identifiers as per <a
   * href=https://github.com/ronsavage/SQL/blob/master/sql-99.bnf>sql-99</a>. This is needed to
   * handle cases where the user might use reserved keywords as column or table names.
   *
   * @param identifier
   * @return
   */
  @VisibleForTesting
  protected static String delimitIdentifier(String identifier) {
    return "\"" + identifier.replaceAll("\"", "\"\"") + "\"";
  }

  private static PartitionColumn partitionColumnFromIndexInfo(SourceColumnIndexInfo idxInfo) {
    return PartitionColumn.builder()
        .setColumnName(delimitIdentifier(idxInfo.columnName()))
        .setColumnClass(indexTypeToColumnClass(idxInfo))
        .setStringCollation(idxInfo.collationReference())
        .setStringMaxLength(idxInfo.stringMaxLength())
        .build();
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
      SourceSchemaReference sourceSchemaReference,
      TableConfig tableConfig,
      SourceTableSchema sourceTableSchema) {
    ReadWithPartitions<SourceRow, @UnknownKeyFor @NonNull @Initialized Long> jdbcIO =
        JdbcIO.<SourceRow>readWithPartitions()
            .withTable(tableConfig.tableName())
            .withPartitionColumn(tableConfig.partitionColumns().get(0).columnName())
            .withDataSourceProviderFn(JdbcIO.PoolableDataSourceProvider.of(dataSourceConfiguration))
            .withRowMapper(
                new JdbcSourceRowMapper(
                    config.valueMappingsProvider(),
                    sourceSchemaReference,
                    sourceTableSchema,
                    config.shardID()));
    if (tableConfig.maxPartitions() != null) {
      jdbcIO = jdbcIO.withNumPartitions(tableConfig.maxPartitions());
    }
    if (config.maxFetchSize() != null) {
      jdbcIO = jdbcIO.withFetchSize(config.maxFetchSize());
    }
    return jdbcIO;
  }

  /**
   * Private helper to construct {@link ReadWithUniformPartitions} as per the reader configuration.
   *
   * @param config Configuration.
   * @param dataSourceConfiguration dataSourceConfiguration (which is derived earlier from the
   *     reader configuration)
   * @param tableConfig discovered table configurations.
   * @param sourceTableSchema schema of the source table.
   * @return
   */
  private static PTransform<PBegin, PCollection<SourceRow>> getReadWithUniformPartitionIO(
      JdbcIOWrapperConfig config,
      DataSourceConfiguration dataSourceConfiguration,
      SourceSchemaReference sourceSchemaReference,
      TableConfig tableConfig,
      SourceTableSchema sourceTableSchema) {

    ReadWithUniformPartitions.Builder<SourceRow> readWithUniformPartitionsBuilder =
        ReadWithUniformPartitions.<SourceRow>builder()
            .setTableName(tableConfig.tableName())
            .setPartitionColumns(tableConfig.partitionColumns())
            .setDataSourceProviderFn(JdbcIO.PoolableDataSourceProvider.of(dataSourceConfiguration))
            .setDbAdapter(config.dialectAdapter())
            .setApproxTotalRowCount(tableConfig.approxRowCount())
            .setFetchSize(config.maxFetchSize())
            .setRowMapper(
                new JdbcSourceRowMapper(
                    config.valueMappingsProvider(),
                    sourceSchemaReference,
                    sourceTableSchema,
                    config.shardID()))
            .setWaitOn(config.waitOn())
            /* The following setting limits number of stages provisioned for the split process.
             * Currently we mostly deal with auto incrementing keys, so we don't need a split depth to make the partition uniform, unless there is a large dataset with a lot of holes.
             * TODO(vardhanvthigle): if index is not of the type of a single auto incrementing key, don't set this.
             */
            .setSplitStageCountHint(0L)
            .setDbParallelizationForSplitProcess(config.dbParallelizationForSplitProcess())
            .setDbParallelizationForReads(config.dbParallelizationForReads())
            .setAdditionalOperationsOnRanges(config.additionalOperationsOnRanges());

    if (tableConfig.maxPartitions() != null) {
      readWithUniformPartitionsBuilder =
          readWithUniformPartitionsBuilder.setMaxPartitionsHint((long) tableConfig.maxPartitions());
    }
    ReadWithUniformPartitions readWithUniformPartitions = readWithUniformPartitionsBuilder.build();
    LOG.info("Configured ReadWithUniformPartitions {} for {}", readWithUniformPartitions, config);
    return readWithUniformPartitions;
  }

  /**
   * Build the {@link DataSourceConfiguration} from the reader configuration.
   *
   * @param config reader configuration.
   * @return {@link DataSourceConfiguration}
   */
  private static DataSourceConfiguration getDataSourceConfiguration(JdbcIOWrapperConfig config) {
    DataSourceConfiguration dataSourceConfig =
        DataSourceConfiguration.create(new JdbcDataSource(config));
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
