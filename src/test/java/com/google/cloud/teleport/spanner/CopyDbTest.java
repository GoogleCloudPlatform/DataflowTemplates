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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.spanner.ddl.RandomDdlGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import org.apache.beam.sdk.PipelineResult;
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
import org.junit.rules.TemporaryFolder;

/**
 * An end to end test that exports and imports a database and verifies that the content is identical
 * This requires an active GCP project with a Spanner instance.
 * Hence this test can only be run locally with a project set up using 'gcloud config'.
 */
@Category(IntegrationTest.class)
public class CopyDbTest {
  private final String sourceDb = "copydb-source";
  private final String destinationDb = "copydb-dest";

  @Rule public final transient TestPipeline exportPipeline = TestPipeline.create();
  @Rule public final transient TestPipeline importPipeline = TestPipeline.create();
  @Rule public final transient TestPipeline comparePipeline = TestPipeline.create();
  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();
  @Rule public final SpannerServerResource spannerServer = new SpannerServerResource();

  @Before
  public void setup() {
    // Just to make sure an old database is not left over.
    spannerServer.dropDatabase(sourceDb);
    spannerServer.dropDatabase(destinationDb);
  }

  @After
  public void teardown() {
    spannerServer.dropDatabase(sourceDb);
    spannerServer.dropDatabase(destinationDb);
  }

  private void createAndPopulate(Ddl ddl, int numBatches) throws Exception {
    try {
      ddl.prettyPrint(System.out);
    } catch (IOException e) {
      e.printStackTrace();
    }

    spannerServer.createDatabase(sourceDb, ddl.statements());
    spannerServer.createDatabase(destinationDb, Collections.emptyList());
    spannerServer.populateRandomData(sourceDb, ddl, numBatches);
  }

  @Test
  public void allTypesSchema() throws Exception {
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
    createAndPopulate(ddl, 100);
    runTest();
  }

  @Test
  public void emptyTables() throws Exception {
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
    createAndPopulate(ddl, 10);

    // Add empty tables.
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
    spannerServer.updateDatabase(sourceDb, emptyTables.createTableStatements());
    runTest();
  }

  @Test
  public void allEmptyTables() throws Exception {
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
  public void emptyDb() throws Exception {
    Ddl ddl = Ddl.builder().build();
    createAndPopulate(ddl, 0);
    runTest();
  }

  @Test
  public void foreignKeys() throws Exception {
    Ddl ddl = Ddl.builder()
        .createTable("Ref")
        .column("id1").int64().endColumn()
        .column("id2").int64().endColumn()
        .primaryKey().asc("id1").asc("id2").end()
        .endTable()
        .createTable("Child")
        .column("id1").int64().endColumn()
        .column("id2").int64().endColumn()
        .column("id3").int64().endColumn()
        .primaryKey().asc("id1").asc("id2").asc("id3").end()
        .interleaveInParent("Ref")
        // Add some foreign keys that are guaranteed to be satisfied due to interleaving
        .foreignKeys(ImmutableList.of(
           "ALTER TABLE `Child` ADD CONSTRAINT `fk1` FOREIGN KEY (`id1`) REFERENCES `Ref` (`id1`)",
           "ALTER TABLE `Child` ADD CONSTRAINT `fk2` FOREIGN KEY (`id2`) REFERENCES `Ref` (`id2`)",
           "ALTER TABLE `Child` ADD CONSTRAINT `fk3` FOREIGN KEY (`id2`) REFERENCES `Ref` (`id2`)",
           "ALTER TABLE `Child` ADD CONSTRAINT `fk4` FOREIGN KEY (`id2`, `id1`) "
               + "REFERENCES `Ref` (`id2`, `id1`)"))
        .endTable()
        .build();

    createAndPopulate(ddl, 100);
    runTest();
  }

  // TODO: enable this test once CHECK constraints are enabled
  // @Test
  public void checkConstraints() throws Exception {
    Ddl ddl = Ddl.builder()
        .createTable("T")
        .column("id").int64().endColumn()
        .column("A").int64().endColumn()
        .primaryKey().asc("id").end()
        .checkConstraints(ImmutableList.of(
           "CONSTRAINT `ck` CHECK(TO_HEX(SHA1(CAST(A AS STRING))) <= '~')"))
        .endTable().build();

    createAndPopulate(ddl, 100);
    runTest();
  }

  public void randomSchema() throws Exception {
    Ddl ddl = RandomDdlGenerator.builder().build().generate();
    createAndPopulate(ddl, 100);
    runTest();
  }

  @Test
  public void randomSchemaNoData() throws Exception {
    Ddl ddl = RandomDdlGenerator.builder().build().generate();
    createAndPopulate(ddl, 0);
    runTest();
  }

  private void runTest() {
    String tmpDirPath = tmpDir.getRoot().getAbsolutePath();
    ValueProvider.StaticValueProvider<String> destination = ValueProvider.StaticValueProvider
        .of(tmpDirPath);
    ValueProvider.StaticValueProvider<String> jobId = ValueProvider.StaticValueProvider
        .of("jobid");
    ValueProvider.StaticValueProvider<String> source = ValueProvider.StaticValueProvider
        .of(tmpDirPath + "/jobid");

    SpannerConfig sourceConfig = spannerServer.getSpannerConfig(sourceDb);
    exportPipeline.apply("Export", new ExportTransform(sourceConfig, destination, jobId));
    PipelineResult exportResult = exportPipeline.run();
    exportResult.waitUntilFinish();

    SpannerConfig destConfig = spannerServer.getSpannerConfig(destinationDb);
    importPipeline.apply(
        "Import",
        new ImportTransform(
            destConfig,
            source,
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();

    PCollection<Long> mismatchCount =
        comparePipeline.apply("Compare", new CompareDatabases(sourceConfig, destConfig));
    PAssert.that(mismatchCount).satisfies((x) -> {
      assertEquals(Lists.newArrayList(x), Lists.newArrayList(0L));
      return null;
    });
    PipelineResult compareResult = comparePipeline.run();
    compareResult.waitUntilFinish();

    Ddl sourceDdl = readDdl(sourceDb);
    Ddl destinationDdl = readDdl(destinationDb);

    assertThat(sourceDdl.prettyPrint(), equalToIgnoringWhiteSpace(destinationDdl.prettyPrint()));
  }

  private Ddl readDdl(String db) {
    DatabaseClient dbClient = spannerServer.getDbClient(db);
    Ddl ddl;
    try (ReadOnlyTransaction ctx = dbClient.readOnlyTransaction()) {
      ddl = new InformationSchemaScanner(ctx).scan();
    }
    return ddl;
  }
}
