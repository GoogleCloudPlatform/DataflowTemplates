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
package com.google.cloud.teleport.v2.templates.utils;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;

/** Helper class to create shadow tables for different source types. */
public class ShadowTableCreator {

  private final SpannerAccessor spannerAccessor;
  private final SpannerAccessor metadataSpannerAccessor;
  private final Dialect dialect;
  private final SpannerConfig spannerConfig;
  private final SpannerConfig metadataConfig;
  private String shadowTablePrefix;

  public ShadowTableCreator(
      SpannerConfig spannerConfig,
      SpannerConfig metadataConfig,
      Dialect dialect,
      String shadowTablePrefix) {
    this.spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    ;
    this.metadataSpannerAccessor = SpannerAccessor.getOrCreate(metadataConfig);
    this.dialect = dialect;
    this.spannerConfig = spannerConfig;
    this.metadataConfig = metadataConfig;
    this.shadowTablePrefix = shadowTablePrefix;
  }

  public void createShadowTablesInSpanner() {

    BatchClient batchClient = spannerAccessor.getBatchClient();
    BatchReadOnlyTransaction context =
        batchClient.batchReadOnlyTransaction(TimestampBound.strong());
    InformationSchemaScanner scanner = new InformationSchemaScanner(context, dialect);
    Ddl informationSchema = scanner.scan();
    List<String> dataTablesWithoutShadowTables = getDataTablesWithNoShadowTables(informationSchema);

    Ddl.Builder shadowTableBuilder = Ddl.builder(dialect);
    for (String dataTableName : dataTablesWithoutShadowTables) {
      Table shadowTable = constructShadowTable(informationSchema, dataTableName, dialect);
      shadowTableBuilder.addTable(shadowTable);
    }
    List<String> createShadowTableStatements = shadowTableBuilder.build().createTableStatements();

    if (createShadowTableStatements.size() == 0) {
      return;
    }

    DatabaseAdminClient databaseAdminClient = metadataSpannerAccessor.getDatabaseAdminClient();

    OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
        databaseAdminClient.updateDatabaseDdl(
            metadataConfig.getInstanceId().get(),
            metadataConfig.getDatabaseId().get(),
            createShadowTableStatements,
            null);

    try {
      op.get(5, TimeUnit.MINUTES);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
    return;
  }

  /*
   * Constructs a shadow table for a data table in the information schema.
   * Note: Shadow tables for interleaved tables are not interleaved to
   * their shadow parent table.
   */
  Table constructShadowTable(Ddl informationSchema, String dataTableName, Dialect dialect) {

    // Create a new shadow table with the given prefix.
    Table.Builder shadowTableBuilder = Table.builder(dialect);
    String shadowTableName = shadowTablePrefix + dataTableName;
    shadowTableBuilder.name(shadowTableName);

    // Add key columns from the data table to the shadow table builder.
    Table dataTable = informationSchema.table(dataTableName);
    Set<String> primaryKeyColNames =
        dataTable.primaryKeys().stream().map(k -> k.name()).collect(Collectors.toSet());
    List<Column> primaryKeyCols =
        dataTable.columns().stream()
            .filter(col -> primaryKeyColNames.contains(col.name()))
            .collect(Collectors.toList());
    for (Column col : primaryKeyCols) {
      shadowTableBuilder.addColumn(col);
    }

    // Add primary key constraints.
    for (IndexColumn keyColumn : dataTable.primaryKeys()) {
      if (keyColumn.order() == IndexColumn.Order.ASC) {
        shadowTableBuilder.primaryKey().asc(keyColumn.name()).end();
      } else if (keyColumn.order() == IndexColumn.Order.DESC) {
        shadowTableBuilder.primaryKey().desc(keyColumn.name()).end();
      }
    }

    // Add processed timestamp column to hold the commit timestamp of change stream record written
    // to source
    // by the pipeline.
    Column.Builder processedCommitTimestampColumnBuilder =
        shadowTableBuilder.column("processed_commit_ts");
    shadowTableBuilder.addColumn(
        processedCommitTimestampColumnBuilder.type(Type.timestamp()).notNull(false).autoBuild());

    return shadowTableBuilder.build();
  }

  /*
   * Returns the list of data table names that don't have a corresponding shadow table.
   */
  List<String> getDataTablesWithNoShadowTables(Ddl ddl) {
    // Get the list of shadow tables in the information schema based on the prefix.
    Set<String> existingShadowTables = getShadowTablesInDdl(ddl);

    List<String> allTables =
        ddl.allTables().stream().map(t -> t.name()).collect(Collectors.toList());

    BatchClient batchClient = metadataSpannerAccessor.getBatchClient();
    BatchReadOnlyTransaction context =
        batchClient.batchReadOnlyTransaction(TimestampBound.strong());
    InformationSchemaScanner scanner = new InformationSchemaScanner(context, dialect);
    Ddl metadataDbinformationSchema = scanner.scan();
    Set<String> existingShadowTablesInMetadataDb =
        getShadowTablesInDdl(metadataDbinformationSchema);
    /*
     * Filter out the following from the list of all table names to get the list of
     * data tables which do not have corresponding shadow tables:
     * (1) Existing shadow tables
     * (2) Data tables which have corresponding shadow tables.
     */
    return allTables.stream()
        .filter(f -> !f.startsWith(shadowTablePrefix))
        .filter(f -> !existingShadowTables.contains(shadowTablePrefix + f))
        .filter(f -> !existingShadowTablesInMetadataDb.contains(shadowTablePrefix + f))
        .collect(Collectors.toList());
  }

  Set<String> getShadowTablesInDdl(Ddl informationSchema) {
    List<String> allTables =
        informationSchema.allTables().stream().map(t -> t.name()).collect(Collectors.toList());
    Set<String> shadowTables =
        allTables.stream().filter(f -> f.startsWith(shadowTablePrefix)).collect(Collectors.toSet());
    return shadowTables;
  }
}
