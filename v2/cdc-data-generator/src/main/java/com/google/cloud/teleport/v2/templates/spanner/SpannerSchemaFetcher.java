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
package com.google.cloud.teleport.v2.templates.spanner;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.migrations.spanner.SpannerSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorUniqueKey;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.sink.SinkSchemaFetcher;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.json.JSONObject;

public class SpannerSchemaFetcher implements SinkSchemaFetcher {

  @FunctionalInterface
  interface DdlFetcher {
    Ddl fetch(SpannerConfig spannerConfig);
  }

  @FunctionalInterface
  interface SpannerAccessorFetcher {
    SpannerAccessor get(SpannerConfig spannerConfig);
  }

  private String projectId;
  private String instanceId;
  private String databaseId;

  private final SpannerTypeMapper typeMapper = new SpannerTypeMapper();

  private final DdlFetcher ddlFetcher;

  public SpannerSchemaFetcher() {
    this(SpannerSchema::getInformationSchemaAsDdl);
  }

  @VisibleForTesting
  SpannerSchemaFetcher(DdlFetcher ddlFetcher) {
    this.ddlFetcher = ddlFetcher;
  }

  @Override
  public void init(String sinkConfigPath) {
    try {
      MatchResult match = FileSystems.match(sinkConfigPath);
      if (match.metadata().isEmpty()) {
        throw new RuntimeException("Spanner sink config file not found: " + sinkConfigPath);
      }
      ResourceId resourceId = match.metadata().get(0).resourceId();
      ReadableByteChannel channel = FileSystems.open(resourceId);
      String content;
      try (BufferedReader reader = new BufferedReader(Channels.newReader(channel, "UTF-8"))) {
        content = reader.lines().collect(Collectors.joining(System.lineSeparator()));
      }
      JSONObject json = new JSONObject(content);
      this.projectId = json.getString("projectId");
      this.instanceId = json.getString("instanceId");
      this.databaseId = json.getString("databaseId");
    } catch (java.io.IOException e) {
      throw new RuntimeException("Error reading Spanner sink config file: " + sinkConfigPath, e);
    }
  }

  @Override
  public DataGeneratorSchema getSchema() throws IOException {
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(StaticValueProvider.of(projectId))
            .withInstanceId(StaticValueProvider.of(instanceId))
            .withDatabaseId(StaticValueProvider.of(databaseId));
    try {
      Ddl ddl = ddlFetcher.fetch(spannerConfig);
      return mapToDataGeneratorSchema(ddl);
    } catch (Exception e) {
      throw new IOException("Failed to fetch Spanner schema", e);
    }
  }

  private DataGeneratorSchema mapToDataGeneratorSchema(Ddl ddl) {
    Map<String, DataGeneratorTable> tables =
        ddl.allTables().stream()
            .collect(
                Collectors.toMap(
                    com.google.cloud.teleport.v2.spanner.ddl.Table::name,
                    table -> mapTable(table, ddl.dialect())));

    return DataGeneratorSchema.builder().tables(ImmutableMap.copyOf(tables)).build();
  }

  private DataGeneratorTable mapTable(
      com.google.cloud.teleport.v2.spanner.ddl.Table table,
      com.google.cloud.spanner.Dialect dialect) {
    List<String> primaryKeys =
        table.primaryKeys().stream().map(IndexColumn::name).collect(Collectors.toList());
    ImmutableList.Builder<DataGeneratorColumn> columnsBuilder = ImmutableList.builder();
    for (com.google.cloud.teleport.v2.spanner.ddl.Column column : table.columns()) {
      columnsBuilder.add(mapColumn(column, table, dialect, primaryKeys));
    }

    ImmutableList.Builder<DataGeneratorForeignKey> fksBuilder = ImmutableList.builder();
    if (table.foreignKeys() != null) {
      for (com.google.cloud.teleport.v2.spanner.ddl.ForeignKey fk : table.foreignKeys()) {
        fksBuilder.add(
            DataGeneratorForeignKey.builder()
                .name(fk.name())
                .referencedTable(fk.referencedTable())
                .keyColumns(fk.columns())
                .referencedColumns(fk.referencedColumns())
                .build());
      }
    }

    ImmutableList.Builder<DataGeneratorUniqueKey> uniqueKeysBuilder = ImmutableList.builder();
    if (table.indexes() != null) {
      for (com.google.cloud.teleport.v2.spanner.ddl.Index index : table.indexes()) {
        // Ignore PRIMARY_KEY and only include unique indices
        if (index.unique() && !"PRIMARY_KEY".equalsIgnoreCase(index.name())) {
          uniqueKeysBuilder.add(
              DataGeneratorUniqueKey.builder()
                  .name(index.name())
                  .columns(
                      index.indexColumns().stream()
                          .filter(
                              c ->
                                  c.order()
                                      != com.google.cloud.teleport.v2.spanner.ddl.IndexColumn.Order
                                          .STORING)
                          .map(com.google.cloud.teleport.v2.spanner.ddl.IndexColumn::name)
                          .collect(ImmutableList.toImmutableList()))
                  .build());
        }
      }
    }

    return DataGeneratorTable.builder()
        .name(table.name())
        .columns(columnsBuilder.build())
        .primaryKeys(
            table.primaryKeys().stream()
                .map(com.google.cloud.teleport.v2.spanner.ddl.IndexColumn::name)
                .collect(ImmutableList.toImmutableList()))
        .interleavedInTable(table.interleavingParent())
        .foreignKeys(fksBuilder.build())
        .uniqueKeys(uniqueKeysBuilder.build())
        .isRoot(table.interleavingParent() == null)
        .insertQps(0)
        .updateQps(0) // Default value
        .deleteQps(0) // Default value
        .recordsPerTick(1.0) // Default value
        .build();
  }

  private DataGeneratorColumn mapColumn(
      com.google.cloud.teleport.v2.spanner.ddl.Column column,
      com.google.cloud.teleport.v2.spanner.ddl.Table table,
      com.google.cloud.spanner.Dialect dialect,
      List<String> primaryKeys) {

    LogicalType logicalType = typeMapper.getLogicalType(column.typeString(), dialect, null);
    Long size = null;
    Integer precision = null;
    Integer scale = null;

    if (column.size() != null) {
      if (column.size() == -1) {
        if (logicalType == LogicalType.STRING) {
          size = 2621440L;
        } else if (logicalType == LogicalType.BYTES) {
          size = 10485760L;
        }
      } else {
        size = Long.valueOf(column.size());
      }
    }

    String typeStr = column.typeString().toUpperCase(Locale.ROOT);
    if (typeStr.contains("BIGNUMERIC")) {
      precision = 76;
      scale = 38;
    } else if (typeStr.contains("NUMERIC") || typeStr.contains("DECIMAL")) {
      precision = 38;
      scale = 9;
    }

    return DataGeneratorColumn.builder()
        .name(column.name())
        .logicalType(logicalType)
        .isNullable(!column.notNull())
        .isGenerated(column.isGenerated())
        .isPrimaryKey(primaryKeys.contains(column.name()))
        .size(size)
        .precision(precision)
        .scale(scale)
        .build();
  }
}
