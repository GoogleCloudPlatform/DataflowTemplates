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
package com.google.cloud.teleport.v2.spanner.migrations.spanner;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ColumnPK;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;

/** Class to add all Spanner schema related functionality. */
public class SpannerSchema {

  public static Ddl getInformationSchemaAsDdl(SpannerConfig spannerConfig) {
    SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    DatabaseAdminClient databaseAdminClient = spannerAccessor.getDatabaseAdminClient();
    Dialect dialect =
            databaseAdminClient
                    .getDatabase(spannerConfig.getInstanceId().get(), spannerConfig.getDatabaseId().get())
                    .getDialect();
    BatchClient batchClient = spannerAccessor.getBatchClient();
    BatchReadOnlyTransaction context =
            batchClient.batchReadOnlyTransaction(TimestampBound.strong());
    InformationSchemaScanner scanner = new InformationSchemaScanner(context, dialect);
    Ddl ddl = scanner.scan();
    spannerAccessor.close();
    return ddl;
  }

  public static Map<String, SpannerTable> convertDDLTableToSpannerTable(Collection<Table> tables) {
    return tables.stream()
            .collect(
                    Collectors.toMap(
                            Table::name, // Use the table name as the key
                            SpannerSchema::convertTableToSpannerTable // Convert Table to SpannerTable
                    ));
  }

  public static Map<String, NameAndCols> convertDDLTableToSpannerNameAndColsTable(
          Collection<Table> tables) {
    return tables.stream()
            .collect(
                    Collectors.toMap(
                            Table::name, // Use the table name as the key
                            SpannerSchema
                                    ::convertTableToSpannerTableNameAndCols // Convert Table to SpannerTable
                    ));
  }

  private static NameAndCols convertTableToSpannerTableNameAndCols(Table table) {
    return new NameAndCols(
            table.name(),
            table.columns().stream()
                    .collect(
                            Collectors.toMap(
                                    Column::name, // Use column IDs as keys
                                    Column::name)));
  }

  private static SpannerTable convertTableToSpannerTable(Table table) {
    String name = table.name(); // Table name
    // Extract column IDs
    String[] colIds =
            table.columns().stream()
                    .map(Column::name) // Assuming Column name as ID
                    .toArray(String[]::new);

    // Build column definitions
    Map<String, SpannerColumnDefinition> colDefs =
            table.columns().stream()
                    .collect(
                            Collectors.toMap(
                                    Column::name, // Use column IDs as keys
                                    column ->
                                            new SpannerColumnDefinition(
                                                    column.name(),
                                                    new SpannerColumnType(
                                                            column.typeString(), // Type Code name (e.g., STRING, INT64)
                                                            false))));

    // Extract primary keys
    AtomicInteger orderCounter = new AtomicInteger(1);
    ColumnPK[] primaryKeys =
            table.primaryKeys().stream()
                    .map(pk -> new ColumnPK(pk.name(), orderCounter.getAndIncrement()))
                    .toArray(ColumnPK[]::new);

    return new SpannerTable(name, colIds, colDefs, primaryKeys, table.name());
  }
}
