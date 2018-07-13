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

import static com.google.cloud.teleport.spanner.Matchers.equalsIgnoreWhitespace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.spanner.ddl.RandomDdlGenerator;
import com.google.cloud.teleport.spanner.ddl.RandomInsertMutationGenerator;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.beam.sdk.PipelineResult;
import com.google.cloud.teleport.spanner.connector.spanner.MutationGroup;
import com.google.cloud.teleport.spanner.connector.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * An end to end test that copies a database and verifies that the content is equal.
 */
public class CopyDbTest {

  private final String instanceId = "mairbek-deleteme";
  private final String sourceDb = "temp2";
  private final String destinationDb = "temp3";

  @Rule public final transient TestPipeline exportPipeline = TestPipeline.create();
  @Rule public final transient TestPipeline importPipeline = TestPipeline.create();
  @Rule public final transient TestPipeline comparePipeline = TestPipeline.create();

  @Before
  public void setup() {
  }

  private void createAndPopulate(Ddl ddl, int numBatches) {
    SpannerOptions spannerOptions = SpannerOptions.newBuilder().build();
    Spanner client = spannerOptions.getService();

    DatabaseAdminClient databaseAdminClient = client.getDatabaseAdminClient();
    try {
      databaseAdminClient.dropDatabase(instanceId, sourceDb);
    } catch (SpannerException e) {
      // Does not exist, ignore.
    }

    try {
      ddl.prettyPrint(System.out);
    } catch (IOException e) {
      e.printStackTrace();
    }

    Operation<Database, CreateDatabaseMetadata> op = databaseAdminClient
        .createDatabase(instanceId, sourceDb, ddl.statements());
    op.waitFor();

    try {
      databaseAdminClient.dropDatabase(instanceId, destinationDb);
    } catch (SpannerException e) {
      // Does not exist, ignore.
    }

    op = databaseAdminClient
        .createDatabase(instanceId, destinationDb, Collections.emptyList());
    op.waitFor();

    DatabaseClient dbClient = client
        .getDatabaseClient(DatabaseId.of(spannerOptions.getProjectId(), instanceId, sourceDb));

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

  @After
  public void teardown() {
  }

  @Test
  public void allTypesSchema() {
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
            .endTable()
            .build();
    createAndPopulate(ddl, 100);
    runTest();
  }

  @Test
  public void emptyTables() {
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
            .endTable()
            .build();
    createAndPopulate(ddl, 100);
    // Add empty tables.
    SpannerOptions spannerOptions = SpannerOptions.newBuilder().build();
    Spanner client = spannerOptions.getService();
    DatabaseAdminClient databaseAdminClient = client.getDatabaseAdminClient();
    Ddl emptyTables = Ddl.builder()
        .createTable("empty_one")
          .column("first").string().max().endColumn()
          .column("second").string().size(5).endColumn()
          .column("value").int64().endColumn()
          .primaryKey().asc("first").desc("second").end()
          .endTable()
        .createTable("empty_two")
          .column("first").string().max().endColumn()
          .column("second").string().size(5).endColumn()
          .column("value").int64().endColumn()
          .column("another_value").int64().endColumn()
          .primaryKey().asc("first").end()
          .endTable()
        .build();
    Operation<Void, UpdateDatabaseDdlMetadata> op = databaseAdminClient
        .updateDatabaseDdl(instanceId, sourceDb, emptyTables.createTableStatements(), null);
    op.waitFor();

    runTest();
  }

  @Test
  public void allEmptyTables() {
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
            .endTable()
            .build();
    createAndPopulate(ddl, 0);
    runTest();
  }

  @Test
  public void emptyDb() {
        Ddl ddl = Ddl.builder()
            .build();
    createAndPopulate(ddl, 0);
    runTest();
  }

  @Test
  public void randomSchema() {
    Ddl ddl = RandomDdlGenerator.builder().build().generate();
    createAndPopulate(ddl, 100);
    runTest();
  }

  @Test
  public void randomSchemaNoData() {
    Ddl ddl = RandomDdlGenerator.builder().build().generate();
    createAndPopulate(ddl, 0);
    runTest();
  }

  private void runTest() {
    String tmpDir = Files.createTempDir().getAbsolutePath();
//    tmpDir = "/Users/mairbek/tmpie/one";
//    tmpDir = "gs://mairbek/df-manitest";
    ValueProvider.StaticValueProvider<String> destination = ValueProvider.StaticValueProvider
        .of(tmpDir);
    ValueProvider.StaticValueProvider<String> jobId = ValueProvider.StaticValueProvider
        .of("jobid");
    ValueProvider.StaticValueProvider<String> source = ValueProvider.StaticValueProvider
        .of(tmpDir + "/jobid");
    SpannerConfig sourceConfig = SpannerConfig.create().withInstanceId(instanceId)
        .withDatabaseId(sourceDb);
    exportPipeline.apply("Export", new ExportTransform(sourceConfig, destination, jobId));
    PipelineResult exportResult = exportPipeline.run();
    exportResult.waitUntilFinish();

    SpannerConfig copyConfig = SpannerConfig.create().withInstanceId(instanceId)
        .withDatabaseId(destinationDb);
    importPipeline.apply("Import", new ImportTransform(
        copyConfig, source, ValueProvider.StaticValueProvider.of(true)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();

    PCollection<Long> mismatchCount = comparePipeline
        .apply("Compare", new CompareDatabases(sourceConfig, copyConfig));
    PAssert.that(mismatchCount).satisfies((x) -> {
      assertEquals(Lists.newArrayList(x), Lists.newArrayList(0L));
      return null;
    });
    PipelineResult compareResult = comparePipeline.run();
    compareResult.waitUntilFinish();

    Ddl sourceDdl = readDdl(sourceDb);
    Ddl destinationDdl = readDdl(destinationDb);

    assertThat(sourceDdl.prettyPrint(), equalsIgnoreWhitespace(destinationDdl.prettyPrint()));
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
