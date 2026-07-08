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
package com.google.cloud.teleport.v2.spanner.sourceddl;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scans a Cloud Spanner database's information schema and converts it into a {@link SourceSchema}.
 *
 * <p>Uses the existing {@link InformationSchemaScanner} to read the target Spanner DDL and then
 * maps each {@link Table} and {@link Column} into the {@link SourceTable}/{@link SourceColumn}
 * model so that the rest of the reverse-replication pipeline can treat Spanner as just another
 * source type.
 */
public class SpannerInformationSchemaScanner implements SourceSchemaScanner {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerInformationSchemaScanner.class);

  private final SpannerConfig spannerConfig;
  private final SourceDatabaseType sourceType = SourceDatabaseType.SPANNER;

  public SpannerInformationSchemaScanner(SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
  }

  @Override
  public SourceSchema scan() {
    SpannerAccessor accessor = SpannerAccessor.getOrCreate(spannerConfig);
    try {
      BatchClient batchClient = accessor.getBatchClient();
      BatchReadOnlyTransaction txn = batchClient.batchReadOnlyTransaction(TimestampBound.strong());
      InformationSchemaScanner scanner = new InformationSchemaScanner(txn);
      Ddl ddl = scanner.scan();
      LOG.info("Scanned Spanner schema for database '{}'", spannerConfig.getDatabaseId().get());
      return convertDdlToSourceSchema(ddl);
    } finally {
      accessor.close();
    }
  }

  SourceSchema convertDdlToSourceSchema(Ddl ddl) {
    Map<String, SourceTable> tables = new HashMap<>();
    for (Table spannerTable : ddl.allTables()) {
      SourceTable sourceTable = convertTable(spannerTable);
      tables.put(sourceTable.name(), sourceTable);
    }
    return SourceSchema.builder(sourceType)
        .databaseName(spannerConfig.getDatabaseId().get())
        .tables(ImmutableMap.copyOf(tables))
        .build();
  }

  SourceTable convertTable(Table spannerTable) {
    List<String> pkColumns = new ArrayList<>();
    for (IndexColumn pk : spannerTable.primaryKeys()) {
      pkColumns.add(pk.name());
    }

    List<SourceColumn> columns = new ArrayList<>();
    for (Column col : spannerTable.columns()) {
      SourceColumn sourceCol =
          SourceColumn.builder(sourceType)
              .name(col.name())
              .type(spannerTypeToString(col.type()))
              .isNullable(!col.notNull())
              .isPrimaryKey(pkColumns.contains(col.name()))
              .isGenerated(col.isGenerated())
              .columnOptions(ImmutableList.of())
              .build();
      columns.add(sourceCol);
    }

    return SourceTable.builder(sourceType)
        .name(spannerTable.name())
        .columns(ImmutableList.copyOf(columns))
        .primaryKeyColumns(ImmutableList.copyOf(pkColumns))
        .foreignKeys(ImmutableList.of())
        .indexes(ImmutableList.of())
        .build();
  }

  /**
   * Converts a Spanner {@link Type} to a canonical type-name string used in {@link SourceColumn}.
   */
  static String spannerTypeToString(Type type) {
    switch (type.getCode()) {
      case ARRAY:
        return "ARRAY<" + spannerTypeToString(type.getArrayElementType()) + ">";
      case PG_ARRAY:
        return "PG_ARRAY<" + spannerTypeToString(type.getArrayElementType()) + ">";
      default:
        return type.getCode().name();
    }
  }
}
