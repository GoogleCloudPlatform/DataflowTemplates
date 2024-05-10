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
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Beam transform which 1) Reads information schema using {@link InformationSchemaScanner}. 2)
 * Create shadow tables in the destination Cloud Spanner database. 3) Outputs the updated
 * information schema.
 */
public class ProcessInformationSchema extends PTransform<PBegin, PCollection<Ddl>> {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessInformationSchema.class);
  private final SpannerConfig spannerConfig;
  private final Boolean shouldCreateShadowTables;
  private final String shadowTablePrefix;
  private final String sourceType;

  public ProcessInformationSchema(
      SpannerConfig spannerConfig,
      Boolean shouldCreateShadowTables,
      String shadowTablePrefix,
      String sourceType) {
    this.spannerConfig = spannerConfig;
    this.shouldCreateShadowTables = shouldCreateShadowTables;
    this.shadowTablePrefix = shadowTablePrefix;
    this.sourceType = sourceType;
  }

  @Override
  public PCollection<Ddl> expand(PBegin p) {
    return p.apply("Create empty", Create.of((Void) null))
        .apply(
            "Create Shadow tables and return Information Schema",
            ParDo.of(
                new ProcessInformationSchemaFn(
                    spannerConfig, shouldCreateShadowTables,
                    shadowTablePrefix, sourceType)));
  }

  static class ProcessInformationSchemaFn extends DoFn<Void, Ddl> {
    private final SpannerConfig spannerConfig;
    private transient SpannerAccessor spannerAccessor;
    private transient Dialect dialect;
    private final Boolean shouldCreateShadowTables;
    private final String shadowTablePrefix;
    private final String sourceType;

    // Timeout for Cloud Spanner schema update.
    private static final int SCHEMA_UPDATE_WAIT_MIN = 5;

    public ProcessInformationSchemaFn(
        SpannerConfig spannerConfig,
        Boolean shouldCreateShadowTables,
        String shadowTablePrefix,
        String sourceType) {
      this.spannerConfig = spannerConfig;
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
    }

    @Teardown
    public void teardown() throws Exception {
      spannerAccessor.close();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (shouldCreateShadowTables) {
        createShadowTablesInSpanner(getInformationSchemaAsDdl());
      }
      c.output(getInformationSchemaAsDdl());
    }

    Ddl getInformationSchemaAsDdl() {
      BatchClient batchClient = spannerAccessor.getBatchClient();
      BatchReadOnlyTransaction context =
          batchClient.batchReadOnlyTransaction(TimestampBound.strong());
      InformationSchemaScanner scanner = new InformationSchemaScanner(context, dialect);
      return scanner.scan();
    }

    void createShadowTablesInSpanner(Ddl informationSchema) {
      List<String> dataTablesWithoutShadowTables =
          getDataTablesWithNoShadowTables(informationSchema);

      Ddl.Builder shadowTableBuilder = Ddl.builder(dialect);
      ShadowTableCreator shadowTableCreator =
          new ShadowTableCreator(sourceType, shadowTablePrefix, dialect);
      for (String dataTableName : dataTablesWithoutShadowTables) {
        Table shadowTable =
            shadowTableCreator.constructShadowTable(informationSchema, dataTableName, dialect);
        shadowTableBuilder.addTable(shadowTable);
      }
      List<String> createShadowTableStatements = shadowTableBuilder.build().createTableStatements();

      if (createShadowTableStatements.size() == 0) {
        return;
      }

      DatabaseAdminClient databaseAdminClient = spannerAccessor.getDatabaseAdminClient();

      OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
          databaseAdminClient.updateDatabaseDdl(
              spannerConfig.getInstanceId().get(),
              spannerConfig.getDatabaseId().get(),
              createShadowTableStatements,
              null);

      try {
        op.get(SCHEMA_UPDATE_WAIT_MIN, TimeUnit.MINUTES);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw new RuntimeException(e);
      }
      return;
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
     * Returns the list of data table names that don't have a corresponding shadow table.
     */
    List<String> getDataTablesWithNoShadowTables(Ddl ddl) {
      // Get the list of shadow tables in the information schema based on the prefix.
      Set<String> existingShadowTables = getShadowTablesInDdl(ddl);

      List<String> allTables =
          ddl.allTables().stream().map(t -> t.name()).collect(Collectors.toList());
      /*
       * Filter out the following from the list of all table names to get the list of
       * data tables which do not have corresponding shadow tables:
       * (1) Existing shadow tables
       * (2) Data tables which have corresponding shadow tables.
       */
      return allTables.stream()
          .filter(f -> !f.startsWith(shadowTablePrefix))
          .filter(f -> !existingShadowTables.contains(shadowTablePrefix + f))
          .collect(Collectors.toList());
    }
  }
}
