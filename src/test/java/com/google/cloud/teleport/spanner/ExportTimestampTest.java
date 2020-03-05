/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.spanner;

import static org.hamcrest.text.IsEqualIgnoringWhiteSpace.equalToIgnoringWhiteSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.spanner.ddl.RandomInsertMutationGenerator;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * An end to end test that exports and imports a database at different times and verfies
 * the behavior of export with timestamp and without timestamp.
 * This requires an active GCP project with a Spanner instance.
 * Hence this test can only be run locally with a project set up using 'gcloud config'.
 */
@Category(IntegrationTest.class)
public class ExportTimestampTest {

  private final String instanceId = "import-export-test";
  static String tmpDir = Files.createTempDir().getAbsolutePath();

  // Multiple pipelines needed as everything is run in a single test
  @Rule public final transient TestPipeline exportPipeline1 = TestPipeline.create();
  @Rule public final transient TestPipeline importPipeline1 = TestPipeline.create();
  @Rule public final transient TestPipeline exportPipeline2 = TestPipeline.create();
  @Rule public final transient TestPipeline importPipeline2 = TestPipeline.create();
  @Rule public final transient TestPipeline exportPipeline3 = TestPipeline.create();
  @Rule public final transient TestPipeline importPipeline3 = TestPipeline.create();
  @Rule public final transient TestPipeline exportPipeline4 = TestPipeline.create();
  @Rule public final transient TestPipeline importPipeline4 = TestPipeline.create();
  @Rule public final transient TestPipeline exportPipeline5 = TestPipeline.create();
  @Rule public final transient TestPipeline importPipeline5 = TestPipeline.create();
  @Rule public final transient TestPipeline comparePipeline1 = TestPipeline.create();
  @Rule public final transient TestPipeline comparePipeline2 = TestPipeline.create();
  @Rule public final transient TestPipeline comparePipeline3 = TestPipeline.create();

  @Before
  public void setup() {
  }

  private void populate(String db, Ddl ddl, int numBatches) throws Exception {
    SpannerOptions spannerOptions = SpannerOptions.newBuilder().build();
    Spanner client = spannerOptions.getService();

    DatabaseClient dbClient = client
        .getDatabaseClient(DatabaseId.of(spannerOptions.getProjectId(), instanceId, db));

    final Iterator<MutationGroup> mutations = new RandomInsertMutationGenerator(ddl).stream()
        .iterator();

    for (int i = 0; i < numBatches; i++) {
      TransactionRunner transactionRunner = dbClient.readWriteTransaction();
      transactionRunner.run(new TransactionRunner.TransactionCallable<Void>() {

        @Nullable
        @Override
        public Void run(TransactionContext transaction) {
          for (int i = 0; i < 10; i++) {
            MutationGroup m = mutations.next();
            transaction.buffer(m);
          }
          return null;
        }
      });
    }
    client.close();
  }

  private void createEmptyDb(String db) throws Exception {
    SpannerOptions spannerOptions = SpannerOptions.newBuilder().build();
    Spanner client = spannerOptions.getService();

    DatabaseAdminClient databaseAdminClient = client.getDatabaseAdminClient();
    try {
      databaseAdminClient.dropDatabase(instanceId, db);
    } catch (SpannerException e) {
      // Does not exist, ignore.
    }
    OperationFuture<Database, CreateDatabaseMetadata> op = databaseAdminClient
        .createDatabase(instanceId, db, Collections.emptyList());
    op.get();
  }

  private void createAndPopulate(String db, Ddl ddl, int numBatches) throws Exception {
    SpannerOptions spannerOptions = SpannerOptions.newBuilder().build();
    Spanner client = spannerOptions.getService();

    DatabaseAdminClient databaseAdminClient = client.getDatabaseAdminClient();
    try {
      databaseAdminClient.dropDatabase(instanceId, db);
    } catch (SpannerException e) {
      // Does not exist, ignore.
    }

    try {
      ddl.prettyPrint(System.out);
    } catch (IOException e) {
      e.printStackTrace();
    }

    OperationFuture<Database, CreateDatabaseMetadata> op =
        databaseAdminClient.createDatabase(instanceId, db, ddl.statements());
    op.get();

    populate(db, ddl, numBatches);

  }

  @After
  public void teardown() {
  }

  String getCurrentTimestamp() {
    Instant instant = Instant.now();
    return instant.toString();
  }

  /* Validates behavior of database export without specifying timestamp
   * and with timestamp specified */
  @Test
  public void runExportWithTsTest() throws Exception {
        Ddl ddl = Ddl.builder()
            .createTable("Users")
              .column("first_name").string().max().endColumn()
              .column("last_name").string().size(5).endColumn()
              .column("age").int64().endColumn()
              .primaryKey().asc("first_name").desc("last_name").end()
            .endTable()
            .createTable("AllTYPES")
              .column("first_name").string().max().endColumn()
              .column("last_name").string().size(5).endColumn()
              .column("id").int64().notNull().endColumn()
              .column("bool_field").bool().endColumn()
              .column("int64_field").int64().endColumn()
              .column("float64_field").float64().endColumn()
              .column("string_field").string().max().endColumn()
              .column("bytes_field").bytes().max().endColumn()
              .column("timestamp_field").timestamp().endColumn()
              .column("date_field").date().endColumn()
              .column("arr_bool_field").type(Type.array(Type.bool())).endColumn()
              .column("arr_int64_field").type(Type.array(Type.int64())).endColumn()
              .column("arr_float64_field").type(Type.array(Type.float64())).endColumn()
              .column("arr_string_field").type(Type.array(Type.string())).max().endColumn()
              .column("arr_bytes_field").type(Type.array(Type.bytes())).max().endColumn()
              .column("arr_timestamp_field").type(Type.array(Type.timestamp())).endColumn()
              .column("arr_date_field").type(Type.array(Type.date())).endColumn()
              .primaryKey().asc("first_name").desc("last_name").asc("id").end()
              .interleaveInParent("Users")
              .onDeleteCascade()
            .endTable()
            .build();
    String sourceDb = "export";
    String destDbPrefix = "import";

    // Create initial table and populate
    createAndPopulate(sourceDb, ddl, 100);

    // Export the database and note the timestamp ts1
    String chkpt1 = "chkpt1";
    createEmptyDb(destDbPrefix + chkpt1);
    exportAndImportDbAtTime(sourceDb, destDbPrefix + chkpt1, chkpt1, "",
                            exportPipeline1, importPipeline1);
    String chkPt1Ts = getCurrentTimestamp();

    Thread.sleep(2000);

    // Sleep for a couple of seconds and note the timestamp ts2
    String chkpt2 = "chkpt2";
    String chkPt2Ts = getCurrentTimestamp();

    Thread.sleep(2000);

    // Add more records to the table, export the database and note the timestamp ts3
    populate(sourceDb, ddl, 100);
    String chkpt3 = "chkpt3";
    createEmptyDb(destDbPrefix + chkpt3);
    exportAndImportDbAtTime(sourceDb, destDbPrefix + chkpt3, chkpt3, "",
                            exportPipeline2, importPipeline2);
    String chkPt3Ts = getCurrentTimestamp();

    // Export timestamp with timestamp ts1
    String chkPt1WithTs = "cp1withts";
    createEmptyDb(destDbPrefix + chkPt1WithTs);
    exportAndImportDbAtTime(sourceDb, destDbPrefix + chkPt1WithTs,
                            chkPt1WithTs, chkPt1Ts,
                            exportPipeline3, importPipeline3);

    // Export timestamp with timestamp ts2
    String chkPt2WithTs = "cp2withts";
    createEmptyDb(destDbPrefix + chkPt2WithTs);
    exportAndImportDbAtTime(sourceDb, destDbPrefix + chkPt2WithTs,
                            chkPt2WithTs, chkPt2Ts,
                            exportPipeline4, importPipeline4);

    // Export timestamp with timestamp ts3
    String chkPt3WithTs = "cp3withts";
    createEmptyDb(destDbPrefix + chkPt3WithTs);
    exportAndImportDbAtTime(sourceDb, destDbPrefix + chkPt3WithTs, chkPt3WithTs, chkPt3Ts,
                            exportPipeline5, importPipeline5);

    // Compare databases exported at ts1 and exported later specifying timestamp ts1
    compareDbs(destDbPrefix + chkpt1, destDbPrefix + chkPt1WithTs, comparePipeline1);
    // Compare databases exported at ts1 and exported later specifying timestamp ts2
    compareDbs(destDbPrefix + chkpt1, destDbPrefix + chkPt2WithTs, comparePipeline2);
    // Compare databases exported at ts3 and exported later specifying timestamp ts3
    compareDbs(destDbPrefix + chkpt3, destDbPrefix + chkPt3WithTs, comparePipeline3);
  }

  private void exportAndImportDbAtTime(String sourceDb, String destDb,
                                       String jobIdName, String ts,
                                       TestPipeline exportPipeline,
                                       TestPipeline importPipeline) {
    ValueProvider.StaticValueProvider<String> destination = ValueProvider.StaticValueProvider
        .of(tmpDir);
    ValueProvider.StaticValueProvider<String> jobId = ValueProvider.StaticValueProvider
        .of(jobIdName);
    ValueProvider.StaticValueProvider<String> source = ValueProvider.StaticValueProvider
        .of(tmpDir + "/" + jobIdName);
    ValueProvider.StaticValueProvider<String> timestamp = ValueProvider.StaticValueProvider.of(ts);
    SpannerConfig sourceConfig = SpannerConfig.create().withInstanceId(instanceId)
        .withDatabaseId(sourceDb);
    exportPipeline.apply("Export", new ExportTransform(sourceConfig, destination,
                                                       jobId, timestamp));
    PipelineResult exportResult = exportPipeline.run();
    exportResult.waitUntilFinish();

    SpannerConfig copyConfig = SpannerConfig.create().withInstanceId(instanceId)
        .withDatabaseId(destDb);
    importPipeline.apply("Import", new ImportTransform(
        copyConfig, source, ValueProvider.StaticValueProvider.of(true),
        ValueProvider.StaticValueProvider.of(true)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();
  }

  private void compareDbs(String sourceDb, String destDb, TestPipeline comparePipeline) {
    SpannerConfig sourceConfig = SpannerConfig.create().withInstanceId(instanceId)
        .withDatabaseId(sourceDb);
    SpannerConfig copyConfig = SpannerConfig.create().withInstanceId(instanceId)
        .withDatabaseId(destDb);
    PCollection<Long> mismatchCount = comparePipeline
        .apply("Compare", new CompareDatabases(sourceConfig, copyConfig));
    PAssert.that(mismatchCount).satisfies((x) -> {
      assertEquals(Lists.newArrayList(x), Lists.newArrayList(0L));
      return null;
    });
    PipelineResult compareResult = comparePipeline.run();
    compareResult.waitUntilFinish();

    Ddl sourceDdl = readDdl(sourceDb);
    Ddl destinationDdl = readDdl(destDb);

    assertThat(sourceDdl.prettyPrint(), equalToIgnoringWhiteSpace(destinationDdl.prettyPrint()));
  }

  private Ddl readDdl(String db) {
    SpannerOptions spannerOptions = SpannerOptions.newBuilder().build();
    Spanner client = spannerOptions.getService();
    DatabaseClient dbClient = client
        .getDatabaseClient(DatabaseId.of(spannerOptions.getProjectId(), instanceId, db));
    Ddl ddl;
    try (ReadOnlyTransaction ctx = dbClient.readOnlyTransaction()) {
      ddl = new InformationSchemaScanner(ctx).scan();
    }
    return ddl;
  }
}
