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
package com.google.cloud.teleport.spanner;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.text.IsEqualCompressingWhiteSpace.equalToCompressingWhiteSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.teleport.spanner.ExportProtos.Export;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.spanner.ddl.RandomDdlGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * An end to end test that exports and imports a database and verifies that the content is
 * identical. Additionally, this test verifies the behavior of table level export. This requires an
 * active GCP project with a Spanner instance. Hence this test can only be run locally with a
 * project set up using 'gcloud config'.
 */
@Category(IntegrationTest.class)
public class CopyDbTest {
  private final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
  private final long numericTime = timestamp.getTime();
  private final String sourceDb = "copydb-source" + Long.toString(numericTime);
  private final String destinationDb = "copydb-dest" + Long.toString(numericTime);
  private final String destDbPrefix = "import";

  @Rule public final transient TestPipeline exportPipeline = TestPipeline.create();
  @Rule public final transient TestPipeline importPipeline = TestPipeline.create();
  @Rule public final transient TestPipeline comparePipeline = TestPipeline.create();
  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();
  @Rule public final SpannerServerResource spannerServer = new SpannerServerResource();

  @After
  public void teardown() {
    spannerServer.dropDatabase(sourceDb);
    spannerServer.dropDatabase(destinationDb);
  }

  private void createAndPopulate(Ddl ddl, int numBatches) throws Exception {
    spannerServer.createDatabase(sourceDb, ddl.statements());
    spannerServer.createDatabase(destinationDb, Collections.emptyList());
    spannerServer.populateRandomData(sourceDb, ddl, numBatches);
  }

  @Test
  public void allTypesSchema() throws Exception {
    // spotless:off
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
    // spotless:on
    createAndPopulate(ddl, 100);
    runTest();
  }

  @Test
  public void emptyTables() throws Exception {
    // spotless:off
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
    // spotless:on
    spannerServer.updateDatabase(sourceDb, emptyTables.createTableStatements());
    runTest();
  }

  @Test
  public void allEmptyTables() throws Exception {
    // spotless:off
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
    // spotless:on
    createAndPopulate(ddl, 0);
    runTest();
  }

  @Test
  public void databaseOptions() throws Exception {
    Ddl.Builder ddlBuilder = Ddl.builder();
    // Table Content
    // spotless:off
    ddlBuilder.createTable("Users")
                .column("first_name").string().max().endColumn()
                .column("last_name").string().size(5).endColumn()
                .column("age").int64().endColumn()
                .primaryKey().asc("first_name").desc("last_name").end()
              .endTable()
              .createTable("EmploymentData")
                .column("first_name").string().max().endColumn()
                .column("last_name").string().size(5).endColumn()
                .column("id").int64().notNull().endColumn()
                .column("age").int64().endColumn()
                .column("address").string().max().endColumn()
                .primaryKey().asc("first_name").desc("last_name").asc("id").end()
                .interleaveInParent("Users")
                .onDeleteCascade()
              .endTable();
    // spotless:on
    // Allowed and well-formed database option
    List<Export.DatabaseOption> dbOptionList = new ArrayList<>();
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("version_retention_period")
            .setOptionValue("\"6d\"")
            .build());
    // Disallowed database option
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("optimizer_version")
            .setOptionValue("1")
            .build());
    // Misformed database option
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("123version")
            .setOptionValue("xyz")
            .build());
    ddlBuilder.mergeDatabaseOptions(dbOptionList);
    Ddl ddl = ddlBuilder.build();
    createAndPopulate(ddl, 100);
    runTest();
    Ddl destinationDdl = readDdl(destinationDb);
    List<String> destDbOptions = destinationDdl.setOptionsStatements(destinationDb);
    assertThat(destDbOptions.size(), is(1));
    assertThat(
        destDbOptions.get(0),
        is(
            "ALTER DATABASE `"
                + destinationDb
                + "` SET OPTIONS ( version_retention_period = \"6d\" )"));
  }

  @Test
  public void emptyDb() throws Exception {
    Ddl ddl = Ddl.builder().build();
    createAndPopulate(ddl, 0);
    runTest();
  }

  @Test
  public void foreignKeys() throws Exception {
    // spotless:off
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
    // spotless:on

    createAndPopulate(ddl, 100);
    runTest();
  }

  // TODO: enable this test once CHECK constraints are enabled
  // @Test
  public void checkConstraints() throws Exception {
    // spotless:off
    Ddl ddl = Ddl.builder()
        .createTable("T")
        .column("id").int64().endColumn()
        .column("A").int64().endColumn()
        .primaryKey().asc("id").end()
        .checkConstraints(ImmutableList.of(
           "CONSTRAINT `ck` CHECK(TO_HEX(SHA1(CAST(A AS STRING))) <= '~')"))
        .endTable().build();
    // spotless:on

    createAndPopulate(ddl, 100);
    runTest();
  }

  @Test
  public void changeStreams() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("T1")
            .endTable()
            .createTable("T2")
            .column("key")
            .int64()
            .endColumn()
            .column("c1")
            .int64()
            .endColumn()
            .column("c2")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("key")
            .end()
            .endTable()
            .createTable("T3")
            .endTable()
            .createChangeStream("ChangeStreamAll")
            .forClause("FOR ALL")
            .options(
                ImmutableList.of(
                    "retention_period=\"7d\"", "value_capture_type=\"OLD_AND_NEW_VALUES\""))
            .endChangeStream()
            .createChangeStream("ChangeStreamEmpty")
            .endChangeStream()
            .createChangeStream("ChangeStreamTableColumns")
            .forClause("FOR `T1`, `T2`(`c1`, `c2`), `T3`()")
            .endChangeStream()
            .build();
    createAndPopulate(ddl, 0);
    runTest();
  }

  @Test
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
    ValueProvider.StaticValueProvider<String> destination =
        ValueProvider.StaticValueProvider.of(tmpDirPath);
    ValueProvider.StaticValueProvider<String> jobId = ValueProvider.StaticValueProvider.of("jobid");
    ValueProvider.StaticValueProvider<String> source =
        ValueProvider.StaticValueProvider.of(tmpDirPath + "/jobid");

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
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(30)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();

    PCollection<Long> mismatchCount =
        comparePipeline.apply("Compare", new CompareDatabases(sourceConfig, destConfig));
    PAssert.that(mismatchCount)
        .satisfies(
            (x) -> {
              assertEquals(Lists.newArrayList(x), Lists.newArrayList(0L));
              return null;
            });
    PipelineResult compareResult = comparePipeline.run();
    compareResult.waitUntilFinish();

    Ddl sourceDdl = readDdl(sourceDb);
    Ddl destinationDdl = readDdl(destinationDb);

    assertThat(sourceDdl.prettyPrint(), equalToCompressingWhiteSpace(destinationDdl.prettyPrint()));
  }

  /* Returns the Ddl representing a Spanner database for given a String for the database name */
  private Ddl readDdl(String db) {
    DatabaseClient dbClient = spannerServer.getDbClient(db);
    Ddl ddl;
    try (ReadOnlyTransaction ctx = dbClient.readOnlyTransaction()) {
      ddl = new InformationSchemaScanner(ctx).scan();
    }
    return ddl;
  }
}
