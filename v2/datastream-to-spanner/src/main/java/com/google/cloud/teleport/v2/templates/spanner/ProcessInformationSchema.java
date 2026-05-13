/*
 * Copyright (C) 2018 Google LLC
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

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * Beam transform which 1) Reads information schema from both main and shadow table database. 2)
 * Create shadow tables in the shadow table database 3) Outputs both DDL schemas
 *
 * <p>The shadow table database housing the shadow tables can be the same as the main database.
 */
public class ProcessInformationSchema extends PTransform<PBegin, PCollectionTuple> {
  public static final TupleTag<Ddl> MAIN_DDL_TAG = new TupleTag<>() {};
  public static final TupleTag<Ddl> SHADOW_TABLE_DDL_TAG = new TupleTag<>() {};
  private final SpannerConfig spannerConfig;
  private final SpannerConfig shadowTableSpannerConfig;
  private final Boolean shouldCreateShadowTables;
  private final String shadowTablePrefix;
  private final String sourceType;

  public ProcessInformationSchema(
      SpannerConfig spannerConfig,
      SpannerConfig shadowTableSpannerConfig,
      Boolean shouldCreateShadowTables,
      String shadowTablePrefix,
      String sourceType) {
    this.spannerConfig = spannerConfig;
    this.shadowTableSpannerConfig = shadowTableSpannerConfig;
    this.shouldCreateShadowTables = shouldCreateShadowTables;
    this.shadowTablePrefix = shadowTablePrefix;
    this.sourceType = sourceType;
  }

  @Override
  public PCollectionTuple expand(PBegin p) {
    return p.apply("Create empty", Create.of((Void) null))
        .apply(
            "Create Shadow tables and return Information Schemas",
            ParDo.of(
                    new ProcessInformationSchemaFn(
                        spannerConfig,
                        shadowTableSpannerConfig,
                        shouldCreateShadowTables,
                        shadowTablePrefix,
                        sourceType))
                .withOutputTags(MAIN_DDL_TAG, TupleTagList.of(SHADOW_TABLE_DDL_TAG)));
  }

  static class ProcessInformationSchemaFn extends DoFn<Void, Ddl> {
    private final SpannerConfig spannerConfig;
    private final SpannerConfig shadowTableSpannerConfig;
    private transient SpannerAccessor spannerAccessor;
    private transient SpannerAccessor shadowTableSpannerAccessor;
    private transient Dialect dialect;
    private final Boolean shouldCreateShadowTables;
    private final String shadowTablePrefix;
    private final String sourceType;

    // Timeout for Cloud Spanner schema update.
    private static final int SCHEMA_UPDATE_WAIT_MIN = 5;

    private boolean useSeparateShadowTableDb = false;

    public ProcessInformationSchemaFn(
        SpannerConfig spannerConfig,
        SpannerConfig shadowTableSpannerConfig,
        Boolean shouldCreateShadowTables,
        String shadowTablePrefix,
        String sourceType) {
      this.spannerConfig = spannerConfig;
      this.shadowTableSpannerConfig = shadowTableSpannerConfig;
      this.shouldCreateShadowTables = shouldCreateShadowTables;
      this.shadowTablePrefix =
          (shadowTablePrefix.endsWith("_")) ? shadowTablePrefix : shadowTablePrefix + "_";
      this.sourceType = sourceType;
    }

    @Setup
    public void setup() throws Exception {
      spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
      DatabaseAdminClient databaseAdminClient = spannerAccessor.getDatabaseAdminClient();
      dialect =
          databaseAdminClient
              .getDatabase(spannerConfig.getInstanceId().get(), spannerConfig.getDatabaseId().get())
              .getDialect();

      // If both point to same database.
      if (spannerConfig.getInstanceId().equals(shadowTableSpannerConfig.getInstanceId())
          && spannerConfig.getDatabaseId().equals(shadowTableSpannerConfig.getDatabaseId())) {
        shadowTableSpannerAccessor = spannerAccessor;
        useSeparateShadowTableDb = false;
      } else {
        shadowTableSpannerAccessor = SpannerAccessor.getOrCreate(shadowTableSpannerConfig);
        useSeparateShadowTableDb = true;
      }
    }

    @Teardown
    public void teardown() throws Exception {
      spannerAccessor.close();
      if (useSeparateShadowTableDb) {
        shadowTableSpannerAccessor.close();
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      // TODO: Add pgsql support/ cross dialect support.
      Ddl mainDdl = getInformationSchemaAsDdl(spannerAccessor);
      Ddl shadowTableDdl = getInformationSchemaAsDdl(shadowTableSpannerAccessor);

      if (shouldCreateShadowTables) {
        createShadowTablesInSpanner(mainDdl, shadowTableDdl);
        // Refresh shadow table DDL after creating new shadow tables
        shadowTableDdl = getInformationSchemaAsDdl(shadowTableSpannerAccessor);
      }
      // Clean up DDLs to ensure proper separation of main and shadow tables. Note that this is only
      // for data hygiene, the downstream methods using these can handle Ddl with excess tables as
      // well.
      mainDdl = cleanupDdl(mainDdl, shadowTablePrefix, /* isMainDdl= */ true);
      shadowTableDdl = cleanupDdl(shadowTableDdl, shadowTablePrefix, /* isMainDdl= */ false);
      c.output(MAIN_DDL_TAG, mainDdl);
      c.output(SHADOW_TABLE_DDL_TAG, shadowTableDdl);
    }

    /**
     * Cleans up the DDL by removing tables that do not match the specified criteria.
     *
     * <p>This method filters the tables in the DDL based on whether they are shadow tables or not.
     * If the method is called to clean up the main DDL, it removes all tables that are shadow
     * tables. If the method is called to clean up the shadow DDL, it removes all tables that are
     * not shadow tables.
     *
     * @param originalDdl The original DDL to be cleaned up.
     * @param shadowPrefix The prefix used to identify shadow tables.
     * @param isMainDdl Indicates whether the DDL being cleaned up is the main DDL or the shadow
     *     DDL.
     * @return A new DDL with the filtered tables.
     */
    Ddl cleanupDdl(Ddl originalDdl, String shadowPrefix, boolean isMainDdl) {
      // Create a new DDL builder with the same dialect as the original
      Ddl.Builder cleanedDdlBuilder = Ddl.builder(dialect);

      // Filter tables based on whether we're cleaning main DDL or shadow DDL
      List<Table> filteredTables =
          originalDdl.allTables().stream()
              .filter(
                  table -> {
                    boolean isTableShadow = table.name().startsWith(shadowPrefix);
                    // For main DDL, keep tables that are NOT shadow tables
                    // For shadow DDL, keep tables that ARE shadow tables
                    return isMainDdl ? !isTableShadow : isTableShadow;
                  })
              .collect(Collectors.toList());

      // Add all filtered tables to the new DDL builder
      filteredTables.forEach(cleanedDdlBuilder::addTable);

      // Build and return the cleaned DDL
      return cleanedDdlBuilder.build();
    }

    Ddl getInformationSchemaAsDdl(SpannerAccessor accessor) {
      BatchClient batchClient = accessor.getBatchClient();
      BatchReadOnlyTransaction context =
          batchClient.batchReadOnlyTransaction(TimestampBound.strong());
      InformationSchemaScanner scanner = new InformationSchemaScanner(context, dialect);
      return scanner.scan();
    }

    void createShadowTablesInSpanner(Ddl mainDdl, Ddl shadowTableDdl) {
      List<String> dataTablesWithoutShadowTables =
          getDataTablesWithNoShadowTables(mainDdl, shadowTableDdl);

      Ddl.Builder shadowTableBuilder = Ddl.builder(dialect);
      ShadowTableCreator shadowTableCreator =
          new ShadowTableCreator(sourceType, shadowTablePrefix, dialect);
      for (String dataTableName : dataTablesWithoutShadowTables) {
        Table shadowTable =
            shadowTableCreator.constructShadowTable(mainDdl, dataTableName, dialect);
        shadowTableBuilder.addTable(shadowTable);
      }
      List<String> createShadowTableStatements = shadowTableBuilder.build().createTableStatements();

      if (createShadowTableStatements.size() == 0) {
        return;
      }

      DatabaseAdminClient databaseAdminClient = shadowTableSpannerAccessor.getDatabaseAdminClient();

      OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
          databaseAdminClient.updateDatabaseDdl(
              shadowTableSpannerConfig.getInstanceId().get(),
              shadowTableSpannerConfig.getDatabaseId().get(),
              createShadowTableStatements,
              null);

      try {
        op.get(SCHEMA_UPDATE_WAIT_MIN, TimeUnit.MINUTES);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw new RuntimeException(e);
      }
    }

    /*
     * Returns the list of shadow tables in Information schema.
     * A table name with the shadow table prefix will determine if its a shadow table.
     */
    Set<String> getShadowTablesInDdl(Ddl informationSchema) {
      List<String> allTables =
          informationSchema.allTables().stream().map(t -> t.name()).collect(Collectors.toList());
      Set<String> shadowTables =
          allTables.stream()
              .filter(f -> f.startsWith(shadowTablePrefix))
              .collect(Collectors.toSet());
      return shadowTables;
    }

    /*
     * Returns the list of data table names from main DB that don't have a corresponding shadow table.
     * Shadow table db could be the same as main db or a separate database. Both of these cases get handled by this method.
     */
    List<String> getDataTablesWithNoShadowTables(Ddl mainDdl, Ddl shadowTableDdl) {
      // Get the list of shadow tables in the shadow table db based on the prefix
      Set<String> existingShadowTables = getShadowTablesInDdl(shadowTableDdl);

      // Get all non-shadow tables from main DB
      List<String> mainTables =
          mainDdl.allTables().stream()
              .map(t -> t.name())
              .filter(f -> !f.startsWith(shadowTablePrefix))
              .collect(Collectors.toList());

      // Return tables that don't have corresponding shadow tables
      return mainTables.stream()
          .filter(f -> !existingShadowTables.contains(shadowTablePrefix + f))
          .collect(Collectors.toList());
    }

    /*
      Added for the purpose of unit testing
    */
    public void setDialect(Dialect dialect) {
      this.dialect = dialect;
    }

    /*
      Added for the purpose of unit testing
    */
    public void setSpannerAccessor(SpannerAccessor spannerAccessor) {
      this.spannerAccessor = spannerAccessor;
    }

    /*
      Added for the purpose of unit testing
    */
    public void setshadowTableSpannerAccessor(SpannerAccessor shadowTableSpannerAccessor) {
      this.shadowTableSpannerAccessor = shadowTableSpannerAccessor;
    }
  }
}
