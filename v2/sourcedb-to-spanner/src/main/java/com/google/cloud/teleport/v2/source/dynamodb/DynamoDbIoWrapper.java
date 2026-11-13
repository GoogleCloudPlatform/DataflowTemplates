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
package com.google.cloud.teleport.v2.source.dynamodb;

import com.google.cloud.teleport.v2.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.DynamoDbConnectionConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** {@link IoWrapper} implementation for Amazon DynamoDB source. */
public final class DynamoDbIoWrapper implements IoWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoDbIoWrapper.class);
  public static final String DYNAMODB_DB_NAME = "dynamodb";
  public static final int DEFAULT_TOTAL_SEGMENTS = 1;

  private final SourceSchema sourceSchema;
  private final ImmutableMap<
          ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
      tableReaders;

  public DynamoDbIoWrapper(
      DynamoDbConnectionConfig config, List<String> sourceTables, @Nullable Integer numPartitions) {
    this(
        config,
        sourceTables,
        numPartitions,
        DynamoDbClientProvider.createClient(config),
        new DynamoDbSchemaDiscovery());
  }

  DynamoDbIoWrapper(
      DynamoDbConnectionConfig config,
      List<String> sourceTables,
      @Nullable Integer numPartitions,
      DynamoDbClient client,
      DynamoDbSchemaDiscovery schemaDiscovery) {
    SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofJdbc(
            JdbcSchemaReference.builder().setDbName(DYNAMODB_DB_NAME).build());
    try (DynamoDbClient dynamoClient = client) {
      ImmutableList<String> tablesToRead =
          getTablesToRead(sourceTables, dynamoClient, schemaDiscovery);
      this.sourceSchema =
          buildSourceSchema(schemaDiscovery, dynamoClient, sourceSchemaReference, tablesToRead);
    }
    int totalSegments = resolveTotalSegments(config, numPartitions);
    ClientConfiguration clientConfiguration =
        DynamoDbClientProvider.createBeamClientConfiguration(config);
    this.tableReaders = buildTableReaders(clientConfiguration, this.sourceSchema, totalSegments);
  }

  static int resolveTotalSegments(
      DynamoDbConnectionConfig config, @Nullable Integer numPartitions) {
    if (numPartitions != null && numPartitions > 0) {
      return numPartitions;
    }
    if (config != null && config.getTotalSegments() != null && config.getTotalSegments() > 0) {
      return config.getTotalSegments();
    }
    return DEFAULT_TOTAL_SEGMENTS;
  }

  static ImmutableList<String> getTablesToRead(
      List<String> sourceTables, DynamoDbClient client, DynamoDbSchemaDiscovery schemaDiscovery) {
    ImmutableList<String> discoveredTables = schemaDiscovery.discoverTables(client);
    if (sourceTables == null || sourceTables.isEmpty()) {
      LOG.info("Auto-discovered DynamoDB tables: {}", discoveredTables);
      return discoveredTables;
    }
    ImmutableSet<String> existingTables = ImmutableSet.copyOf(discoveredTables);
    ImmutableList<String> filteredTables =
        sourceTables.stream()
            .filter(existingTables::contains)
            .collect(ImmutableList.toImmutableList());
    LOG.info(
        "Filtered requested DynamoDB tables {} against existing tables {}: {}",
        sourceTables,
        existingTables,
        filteredTables);
    return filteredTables;
  }

  static SourceSchema buildSourceSchema(
      DynamoDbSchemaDiscovery schemaDiscovery,
      DynamoDbClient client,
      SourceSchemaReference sourceSchemaReference,
      ImmutableList<String> tables) {
    SourceSchema.Builder sourceSchemaBuilder =
        SourceSchema.builder().setSchemaReference(sourceSchemaReference);
    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> tableSchemas =
        schemaDiscovery.discoverTableSchema(client, tables);
    for (Map.Entry<String, ImmutableMap<String, SourceColumnType>> tableEntry :
        tableSchemas.entrySet()) {
      SourceTableSchema.Builder tableSchemaBuilder =
          SourceTableSchema.builder(new UnifiedTypeMapper(DynamoDbMappingsProvider.getMapping()))
              .setTableName(tableEntry.getKey());
      for (Map.Entry<String, SourceColumnType> colEntry : tableEntry.getValue().entrySet()) {
        tableSchemaBuilder.addSourceColumnNameToSourceColumnType(
            colEntry.getKey(), colEntry.getValue());
      }
      sourceSchemaBuilder.addTableSchema(tableSchemaBuilder.build());
    }
    return sourceSchemaBuilder.build();
  }

  static ImmutableMap<
          ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
      buildTableReaders(
          ClientConfiguration clientConfiguration, SourceSchema sourceSchema, int totalSegments) {
    ImmutableMap.Builder<
            ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
        tableReadersBuilder = ImmutableMap.builder();
    SourceSchemaReference sourceSchemaReference = sourceSchema.schemaReference();
    for (SourceTableSchema tableSchema : sourceSchema.tableSchemas()) {
      SourceTableReference sourceTableReference =
          SourceTableReference.builder()
              .setSourceSchemaReference(sourceSchemaReference)
              .setSourceTableSchemaUUID(tableSchema.tableSchemaUUID())
              .setSourceTableName(tableSchema.tableName())
              .build();
      DynamoDbTableReader tableReader =
          new DynamoDbTableReader(
              clientConfiguration, sourceSchemaReference, tableSchema, totalSegments);
      tableReadersBuilder.put(ImmutableList.of(sourceTableReference), tableReader);
    }
    return tableReadersBuilder.build();
  }

  @Override
  public ImmutableMap<
          ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
      getTableReaders() {
    return tableReaders;
  }

  @Override
  public ImmutableList<SourceSchema> discoverTableSchema() {
    return ImmutableList.of(sourceSchema);
  }
}
