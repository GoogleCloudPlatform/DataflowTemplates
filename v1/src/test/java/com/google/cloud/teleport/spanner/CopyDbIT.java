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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.RandomDdlGenerator;
import com.google.cloud.teleport.spanner.ddl.RandomInsertMutationGenerator;
import com.google.cloud.teleport.spanner.ddl.Udf.SqlSecurity;
import com.google.cloud.teleport.spanner.ddl.UdfParameter;
import com.google.cloud.teleport.spanner.proto.ExportProtos.Export;
import com.google.cloud.teleport.spanner.spannerio.MutationGroup;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.dataflow.DirectRunnerClient;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerTemplateITBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * An end to end test that exports and imports a database and verifies that the content is
 * identical. This test utilizes DirectRunner to launch the actual Export and Import templates locally.
 */
@Category({TemplateIntegrationTest.class})
@TemplateIntegrationTest(ExportPipeline.class)
@RunWith(JUnit4.class)
public class CopyDbIT extends SpannerTemplateITBase {

  static {
    // Force direct runner for this test as it tests the local copy process
    System.setProperty("directRunnerTest", "true");
  }

  private SpannerResourceManager sourceResourceManager;
  private SpannerResourceManager destResourceManager;

  @Before
  public void setup() {
    // Initialize resource managers for source and destination Spanner instances
    sourceResourceManager =
        SpannerResourceManager.builder("src-" + testName, PROJECT, REGION, Dialect.GOOGLE_STANDARD_SQL)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .build();

    destResourceManager =
        SpannerResourceManager.builder("dst-" + testName, PROJECT, REGION, Dialect.GOOGLE_STANDARD_SQL)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .build();
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(sourceResourceManager, destResourceManager);
  }

  private void createAndPopulate(Ddl ddl, int numBatches) throws Exception {
    createAndPopulate(ddl, numBatches, Dialect.GOOGLE_STANDARD_SQL);
  }

  private void createAndPopulate(Ddl ddl, int numBatches, Dialect dialect) throws Exception {
    if (dialect == Dialect.POSTGRESQL) {
      sourceResourceManager = SpannerResourceManager.builder("src-" + testName, PROJECT, REGION, Dialect.POSTGRESQL)
          .maybeUseStaticInstance().useCustomHost(spannerHost).build();
      destResourceManager = SpannerResourceManager.builder("dst-" + testName, PROJECT, REGION, Dialect.POSTGRESQL)
          .maybeUseStaticInstance().useCustomHost(spannerHost).build();
    }

    sourceResourceManager.executeDdlStatements(ddl.statements());
    destResourceManager.executeDdlStatements(Collections.emptyList());

    if (numBatches > 0) {
      final Iterator<MutationGroup> mutations =
          new RandomInsertMutationGenerator(ddl).stream().iterator();
      for (int i = 0; i < numBatches; i++) {
        List<com.google.cloud.spanner.Mutation> batch = new ArrayList<>();
        for (int j = 0; j < 10; j++) {
          if (mutations.hasNext()) {
             MutationGroup group = mutations.next();
             group.iterator().forEachRemaining(batch::add);
          }
        }
        sourceResourceManager.write(batch);
      }
    }
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
          .column("float32_field").float32().endColumn()
          .column("float64_field").float64().endColumn()
          .column("string_field").string().max().endColumn()
          .column("bytes_field").bytes().max().endColumn()
          .column("timestamp_field").timestamp().endColumn()
          .column("date_field").date().endColumn()
          .column("arr_bool_field").type(Type.array(Type.bool())).endColumn()
          .column("arr_int64_field").type(Type.array(Type.int64())).endColumn()
          .column("arr_float32_field").type(Type.array(Type.float32())).endColumn()
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
          .column("float32_field").float32().endColumn()
          .column("float64_field").float64().endColumn()
          .column("string_field").string().max().endColumn()
          .column("bytes_field").bytes().max().endColumn()
          .column("timestamp_field").timestamp().endColumn()
          .column("date_field").date().endColumn()
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
    sourceResourceManager.executeDdlStatements(emptyTables.createTableStatements());
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
        .build();
    // spotless:on
    createAndPopulate(ddl, 0);
    runTest();
  }

  @Test
  public void databaseOptions() throws Exception {
    Ddl.Builder ddlBuilder = Ddl.builder();
    // spotless:off
    ddlBuilder.createTable("Users")
        .column("first_name").string().max().endColumn()
        .column("last_name").string().size(5).endColumn()
        .column("age").int64().endColumn()
        .primaryKey().asc("first_name").desc("last_name").end()
        .endTable();
    // spotless:on
    
    List<Export.DatabaseOption> dbOptionList = new ArrayList<>();
    dbOptionList.add(Export.DatabaseOption.newBuilder().setOptionName("version_retention_period").setOptionValue("\"6d\"").build());
    ddlBuilder.mergeDatabaseOptions(dbOptionList);
    Ddl ddl = ddlBuilder.build();
    createAndPopulate(ddl, 10);
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
        .foreignKeys(ImmutableList.of(
           "ALTER TABLE `Child` ADD CONSTRAINT `fk1` FOREIGN KEY (`id1`) REFERENCES `Ref` (`id1`)",
           "ALTER TABLE `Child` ADD CONSTRAINT `fk2` FOREIGN KEY (`id2`) REFERENCES `Ref` (`id2`)"))
        .endTable()
        .build();
    // spotless:on
    createAndPopulate(ddl, 100);
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

  private void runTest() throws Exception {
    runTest(Dialect.GOOGLE_STANDARD_SQL);
  }

  private void runTest(Dialect dialect) throws Exception {
    String gcsPath = getGcsPath("output/");
    
    // 1. Export
    PipelineLauncher exportLauncher = DirectRunnerClient.builder(ExportPipeline.class).build();
    PipelineOperator exportOperator = new PipelineOperator(exportLauncher);
    PipelineLauncher.LaunchConfig.Builder exportOptions =
        PipelineLauncher.LaunchConfig.builder("export-" + testName, "")
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("instanceId", sourceResourceManager.getInstanceId())
            .addParameter("databaseId", sourceResourceManager.getDatabaseId())
            .addParameter("outputDir", gcsPath)
            .addParameter("spannerHost", sourceResourceManager.getSpannerHost());

    PipelineLauncher.LaunchInfo exportInfo = exportLauncher.launch(PROJECT, REGION, exportOptions.build());
    PipelineOperator.Result exportResult = exportOperator.waitUntilDone(PipelineOperator.Config.builder().setJobId(exportInfo.jobId()).setProject(PROJECT).setRegion(REGION).build());
    assertThatResult(exportResult).isLaunchFinished();

    // 2. Import
    PipelineLauncher importLauncher = DirectRunnerClient.builder(ImportPipeline.class).build();
    PipelineOperator importOperator = new PipelineOperator(importLauncher);
    PipelineLauncher.LaunchConfig.Builder importOptions =
        PipelineLauncher.LaunchConfig.builder("import-" + testName, "")
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("instanceId", destResourceManager.getInstanceId())
            .addParameter("databaseId", destResourceManager.getDatabaseId())
            .addParameter("inputDir", gcsPath)
            .addParameter("spannerHost", destResourceManager.getSpannerHost())
            .addParameter("waitForIndexes", "true");

    PipelineLauncher.LaunchInfo importInfo = importLauncher.launch(PROJECT, REGION, importOptions.build());
    PipelineOperator.Result importResult = importOperator.waitUntilDone(PipelineOperator.Config.builder().setJobId(importInfo.jobId()).setProject(PROJECT).setRegion(REGION).build());
    assertThatResult(importResult).isLaunchFinished();

    // 3. Compare Data
    List<Struct> tables = sourceResourceManager.runQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = ''");
    for (Struct row : tables) {
      String tableName = row.getString(0);
      List<Struct> sourceRecords = sourceResourceManager.runQuery("SELECT * FROM `" + tableName + "`");
      List<Struct> destRecords = destResourceManager.runQuery("SELECT * FROM `" + tableName + "`");
      
      assertThat(destRecords.size()).isEqualTo(sourceRecords.size());
      if (!sourceRecords.isEmpty()) {
         assertThat(destRecords).containsExactlyElementsIn(sourceRecords);
      }
    }
  }
  @Test
  public void allPgTypesSchema() throws Exception {
    // spotless:off
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("Users")
            .column("first_name")
            .pgVarchar()
            .max()
            .endColumn()
            .column("last_name")
            .pgVarchar()
            .size(5)
            .endColumn()
            .column("age")
            .pgInt8()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .asc("last_name")
            .end()
            .endTable()
            .createTable("AllTYPES")
            .column("id")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("first_name")
            .pgVarchar()
            .max()
            .endColumn()
            .column("last_name")
            .pgVarchar()
            .size(5)
            .endColumn()
            .column("bool_field")
            .pgBool()
            .endColumn()
            .column("int_field")
            .pgInt8()
            .endColumn()
            .column("float32_field")
            .pgFloat4()
            .endColumn()
            .column("float64_field")
            .pgFloat8()
            .endColumn()
            .column("string_field")
            .pgText()
            .endColumn()
            .column("bytes_field")
            .pgBytea()
            .endColumn()
            .column("timestamp_field")
            .pgTimestamptz()
            .endColumn()
            .column("numeric_field")
            .pgNumeric()
            .endColumn()
            .column("date_field")
            .pgDate()
            .endColumn()
            .column("arr_bool_field")
            .type(Type.pgArray(Type.pgBool()))
            .endColumn()
            .column("arr_int_field")
            .type(Type.pgArray(Type.pgInt8()))
            .endColumn()
            .column("arr_float32_field")
            .type(Type.pgArray(Type.pgFloat4()))
            .endColumn()
            .column("arr_float64_field")
            .type(Type.pgArray(Type.pgFloat8()))
            .endColumn()
            .column("arr_string_field")
            .type(Type.pgArray(Type.pgVarchar()))
            .max()
            .endColumn()
            .column("arr_bytes_field")
            .type(Type.pgArray(Type.pgBytea()))
            .max()
            .endColumn()
            .column("arr_timestamp_field")
            .type(Type.pgArray(Type.pgTimestamptz()))
            .endColumn()
            .column("arr_date_field")
            .type(Type.pgArray(Type.pgDate()))
            .endColumn()
            .column("arr_numeric_field")
            .type(Type.pgArray(Type.pgNumeric()))
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .asc("last_name")
            .asc("id")
            .asc("float64_field")
            .end()
            .interleaveInParent("Users")
            .onDeleteCascade()
            .endTable()
            .build();
    // spotless:on
    createAndPopulate(ddl, 100);
    runTest(Dialect.POSTGRESQL);
  }
  @Test
  public void emptyPgTables() throws Exception {
    // spotless:off
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("Users")
            .column("first_name")
            .pgVarchar()
            .max()
            .endColumn()
            .column("last_name").pgVarchar().size(5).endColumn()
            .column("age")
            .pgInt8()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .asc("last_name")
            .end()
            .endTable()
            .createTable("AllTYPES")
            .column("first_name")
            .pgVarchar()
            .max()
            .endColumn()
            .column("last_name").pgVarchar().size(5).endColumn()
            .column("id")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("bool_field")
            .pgBool()
            .endColumn()
            .column("int_field")
            .pgInt8()
            .endColumn()
            .column("float32_field")
            .pgFloat4()
            .endColumn()
            .column("float64_field")
            .pgFloat8()
            .endColumn()
            .column("string_field")
            .pgText()
            .endColumn()
            .column("bytes_field")
            .pgBytea()
            .endColumn()
            .column("timestamp_field")
            .pgTimestamptz()
            .endColumn()
            .column("numeric_field")
            .pgNumeric()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .asc("last_name")
            .asc("id")
            .end()
            .interleaveInParent("Users")
            .onDeleteCascade()
            .endTable()
            .build();
    createAndPopulate(ddl, 10);

    // Add empty tables.
    Ddl emptyTables =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("empty_one")
            .column("first")
            .pgVarchar()
            .max()
            .endColumn()
            .column("second").pgVarchar().size(5).endColumn()
            .column("value")
            .pgInt8()
            .endColumn()
            .primaryKey()
            .asc("first")
            .asc("second")
            .end()
            .endTable()
            .createTable("empty_two")
            .column("first")
            .pgVarchar()
            .max()
            .endColumn()
            .column("second").pgVarchar().size(5).endColumn()
            .column("value")
            .pgInt8()
            .endColumn()
            .column("another_value")
            .pgInt8()
            .endColumn()
            .primaryKey()
            .asc("first")
            .end()
            .endTable()
            .build();
    // spotless:on
    sourceResourceManager.executeDdlStatements(emptyTables.createTableStatements());
    runTest(Dialect.POSTGRESQL);
  }
  @Test
  public void allEmptyPgTables() throws Exception {
    // spotless:off
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("Users")
            .column("first_name")
            .pgVarchar()
            .max()
            .endColumn()
            .column("last_name")
            .pgVarchar()
            .size(5)
            .endColumn()
            .column("age")
            .pgInt8()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .asc("last_name")
            .end()
            .endTable()
            .createTable("AllTYPES")
            .column("first_name")
            .pgVarchar()
            .max()
            .endColumn()
            .column("last_name")
            .pgVarchar()
            .size(5)
            .endColumn()
            .column("id")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("bool_field")
            .pgBool()
            .endColumn()
            .column("int_field")
            .pgInt8()
            .endColumn()
            .column("float32_field")
            .pgFloat4()
            .endColumn()
            .column("float64_field")
            .pgFloat8()
            .endColumn()
            .column("string_field")
            .pgText()
            .endColumn()
            .column("bytes_field")
            .pgBytea()
            .endColumn()
            .column("timestamp_field")
            .pgTimestamptz()
            .endColumn()
            .column("numeric_field")
            .pgNumeric()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .asc("last_name")
            .asc("id")
            .end()
            .interleaveInParent("Users")
            .onDeleteCascade()
            .endTable()
            .build();
    // spotless:on
    createAndPopulate(ddl, 0);
    runTest(Dialect.POSTGRESQL);
  }
  @Test
  public void pgDatabaseOptions() throws Exception {
    Ddl.Builder ddlBuilder = Ddl.builder(Dialect.POSTGRESQL);
    // Table Content
    // spotless:off
    ddlBuilder
        .createTable("Users")
        .column("first_name")
        .pgVarchar()
        .max()
        .endColumn()
        .column("last_name").pgVarchar().size(5).endColumn()
        .column("age")
        .pgInt8()
        .endColumn()
        .primaryKey()
        .asc("first_name")
        .asc("last_name")
        .end()
        .endTable()
        .createTable("EmploymentData")
        .column("first_name")
        .pgVarchar()
        .max()
        .endColumn()
        .column("last_name").pgVarchar().size(5).endColumn()
        .column("id")
        .pgInt8()
        .notNull()
        .endColumn()
        .column("age")
        .pgInt8()
        .endColumn()
        .column("address")
        .pgVarchar()
        .max()
        .endColumn()
        .primaryKey()
        .asc("first_name")
        .asc("last_name")
        .asc("id")
        .end()
        .interleaveInParent("Users")
        .onDeleteCascade()
        .endTable();
    // spotless:on
    // Allowed and well-formed database option
    List<Export.DatabaseOption> dbOptionList = new ArrayList<>();
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("version_retention_period")
            .setOptionValue("'6d'")
            .build());
    // Disallowed database option
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("optimizer_version")
            .setOptionValue("1")
            .build());
    // Malformed database option
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("123version")
            .setOptionValue("xyz")
            .build());
    ddlBuilder.mergeDatabaseOptions(dbOptionList);
    Ddl ddl = ddlBuilder.build();
    createAndPopulate(ddl, 100);
    runTest(Dialect.POSTGRESQL);
  }
  @Test
  public void emptyDb() throws Exception {
    Ddl ddl = Ddl.builder().build();
    createAndPopulate(ddl, 0);
    runTest();
  }
  @Test
  public void emptyPgDb() throws Exception {
    Ddl ddl = Ddl.builder(Dialect.POSTGRESQL).build();
    createAndPopulate(ddl, 0);
    runTest(Dialect.POSTGRESQL);
  }
  @Test
  public void pgForeignKeys() throws Exception {
    // spotless:off
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("Ref")
            .column("id1")
            .pgInt8()
            .endColumn()
            .column("id2")
            .pgInt8()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .end()
            .endTable()
            .createTable("Child")
            .column("id1")
            .pgInt8()
            .endColumn()
            .column("id2")
            .pgInt8()
            .endColumn()
            .column("id3")
            .pgInt8()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .interleaveInParent("Ref")
            // Add some foreign keys that are guaranteed to be satisfied due to interleaving
            .foreignKeys(
                ImmutableList.of(
                    "ALTER TABLE \"Child\" ADD CONSTRAINT \"fk1\" FOREIGN KEY (\"id1\") REFERENCES"
                        + " \"Ref\" (\"id1\")",
                    "ALTER TABLE \"Child\" ADD CONSTRAINT \"fk2\" FOREIGN KEY (\"id2\") REFERENCES"
                        + " \"Ref\" (\"id2\")",
                    "ALTER TABLE \"Child\" ADD CONSTRAINT \"fk3\" FOREIGN KEY (\"id2\") REFERENCES"
                        + " \"Ref\" (\"id2\")",
                    "ALTER TABLE \"Child\" ADD CONSTRAINT \"fk4\" FOREIGN KEY (\"id2\", \"id1\") "
                        + "REFERENCES \"Ref\" (\"id2\", \"id1\")"))
            .endTable()
            .build();
    // spotless:on

    createAndPopulate(ddl, 100);
    runTest(Dialect.POSTGRESQL);
  }

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
  public void pgCheckConstraints() throws Exception {
    // spotless:off
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("T")
            .column("id")
            .pgInt8()
            .endColumn()
            .column("A")
            .pgInt8()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .checkConstraints(
                ImmutableList.of(
                    "CONSTRAINT \"ck\" CHECK(LENGTH(CAST(\"A\" AS VARCHAR)) >= '0'::bigint)"))
            .endTable()
            .build();
    // spotless:on

    createAndPopulate(ddl, 100);
    runTest(Dialect.POSTGRESQL);
  }
  @Test
  public void models() throws Exception {
    // spotless:off
    Ddl ddl =
        Ddl.builder()
            .createModel("Iris")
            .remote(true)
            .options(ImmutableList.of(
                "endpoint=\"//aiplatform.googleapis.com/projects/span-cloud-testing/locations/us-central1/endpoints/4608339105032437760\""))
            .inputColumn("f1").type(Type.float64()).size(-1).endInputColumn()
            .inputColumn("f2").type(Type.float64()).size(-1).endInputColumn()
            .inputColumn("f3").type(Type.float64()).size(-1).endInputColumn()
            .inputColumn("f4").type(Type.float64()).size(-1).endInputColumn()
            .outputColumn("classes").type(Type.array(Type.string())).size(-1).endOutputColumn()
            .outputColumn("scores").type(Type.array(Type.float64())).size(-1).endOutputColumn()
            .endModel()
            .createModel("TextEmbeddingGecko")
            .remote(true)
            .options(ImmutableList.of(
                "endpoint=\"//aiplatform.googleapis.com/projects/span-cloud-testing/locations/us-central1/publishers/google/models/textembedding-gecko\""))
            .inputColumn("content").type(Type.string()).size(-1).endInputColumn()
            .outputColumn("embeddings").type(Type.struct(
                Type.StructField.of("statistics", Type.struct(Type.StructField.of("truncated", Type.bool()),
                    Type.StructField.of("token_count", Type.float64()))),
                Type.StructField.of("values", Type.array(Type.float64())))).size(-1).endOutputColumn()
            .endModel()
            .build();
    // spotless:on

    createAndPopulate(ddl, 0);
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

  // @Test
  public void pgChangeStreams() throws Exception {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("T1")
            .column("key")
            .pgInt8()
            .endColumn()
            .primaryKey()
            .asc("key")
            .end()
            .endTable()
            .createTable("T2")
            .column("key")
            .pgInt8()
            .endColumn()
            .column("c1")
            .pgInt8()
            .endColumn()
            .column("c2")
            .pgVarchar()
            .max()
            .endColumn()
            .primaryKey()
            .asc("key")
            .end()
            .endTable()
            .createTable("T3")
            .column("key")
            .pgInt8()
            .endColumn()
            .primaryKey()
            .asc("key")
            .end()
            .endTable()
            .createChangeStream("ChangeStreamAll")
            .forClause("FOR ALL")
            .options(
                ImmutableList.of(
                    "retention_period='7d'", "value_capture_type='OLD_AND_NEW_VALUES'"))
            .endChangeStream()
            .createChangeStream("ChangeStreamEmpty")
            .endChangeStream()
            .createChangeStream("ChangeStreamTableColumns")
            .forClause("FOR \"T1\", \"T2\"(\"c1\", \"c2\"), \"T3\"()")
            .endChangeStream()
            .build();
    createAndPopulate(ddl, 0);
    runTest(Dialect.POSTGRESQL);
  }
  @Test
  public void identityColumn() throws Exception {
    // spotless:off
    Ddl.Builder ddlBuilder = Ddl.builder();
    List<Export.DatabaseOption> dbOptionList = new ArrayList<>();
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("default_sequence_kind")
            .setOptionValue("\"bit_reversed_positive\"")
            .build());
    ddlBuilder.mergeDatabaseOptions(dbOptionList);
    Ddl ddl = ddlBuilder
        .createTable("IdentityTable")
          .column("id")
            .int64()
            .isIdentityColumn(true)
            .sequenceKind("bit_reversed_positive")
            .counterStartValue(1000L)
            .skipRangeMin(2000L)
            .skipRangeMax(3000L)
          .endColumn()
          .column("non_key_column")
            .int64()
            .isIdentityColumn(true)
            .sequenceKind("bit_reversed_positive")
            .counterStartValue(1000L)
            .skipRangeMin(2000L)
            .skipRangeMax(3000L)
          .endColumn()
          .column("no_sequence_kind_column")
            .int64()
            .isIdentityColumn(true)
            .sequenceKind("default")
            .counterStartValue(1000L)
            .skipRangeMin(2000L)
            .skipRangeMax(3000L)
          .endColumn()
          .column("value").int64().endColumn()
          .primaryKey().asc("id").end()
        .endTable()
        .build();
    // spotless:on

    createAndPopulate(ddl, 10);
    runTest();
  }
  @Test
  public void pgIdentityColumn() throws Exception {
    // spotless:off
    Ddl.Builder ddlBuilder = Ddl.builder(Dialect.POSTGRESQL);
    List<Export.DatabaseOption> dbOptionList = new ArrayList<>();
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("default_sequence_kind")
            .setOptionValue("\"bit_reversed_positive\"")
            .build());
    ddlBuilder.mergeDatabaseOptions(dbOptionList);
    Ddl ddl = ddlBuilder
        .createTable("IdentityTable")
          .column("id")
            .int64()
            .isIdentityColumn(true)
            .sequenceKind("bit_reversed_positive")
            .counterStartValue(1000L)
            .skipRangeMin(2000L)
            .skipRangeMax(3000L)
          .endColumn()
          .column("non_key_column")
            .int64()
            .isIdentityColumn(true)
            .sequenceKind("bit_reversed_positive")
            .counterStartValue(1000L)
            .skipRangeMin(2000L)
            .skipRangeMax(3000L)
          .endColumn()
          .column("no_sequence_kind_column")
            .int64()
            .isIdentityColumn(true)
            .sequenceKind("default")
            .counterStartValue(1000L)
            .skipRangeMin(2000L)
            .skipRangeMax(3000L)
          .endColumn()
          .column("value").int64().endColumn()
          .primaryKey().asc("id").end()
        .endTable()
        .build();
    // spotless:on

    createAndPopulate(ddl, 10);
    runTest(Dialect.POSTGRESQL);
  }
  @Test
  public void commitTimestampColumns() throws Exception {
    // spotless:off
    Ddl.Builder ddlBuilder = Ddl.builder();
    List<Export.DatabaseOption> dbOptionList = new ArrayList<>();
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("default_sequence_kind")
            .setOptionValue("\"bit_reversed_positive\"")
            .build());
    ddlBuilder.mergeDatabaseOptions(dbOptionList);
    Ddl ddl = ddlBuilder
              .createTable("CommitTimestampTable")
              .column("id")
              .int64()
              .endColumn()
              .column("default_commit_ts")
              .type(Type.timestamp())
              .defaultExpression("PENDING_COMMIT_TIMESTAMP()")
              .columnOptions(ImmutableList.of("allow_commit_timestamp=TRUE"))
              .endColumn()
              .column("on_update_ts")
              .type(Type.timestamp())
              .defaultExpression("PENDING_COMMIT_TIMESTAMP()")
              .onUpdateExpression("PENDING_COMMIT_TIMESTAMP()")
              .columnOptions(ImmutableList.of("allow_commit_timestamp=TRUE"))
              .endColumn()
              .primaryKey().asc("id").end()
              .endTable()
              .build();
    // spotless:on

    createAndPopulate(ddl, 10);
    runTest();
  }
  @Test
  public void pgCommitTimestampColumns() throws Exception {
    // spotless:off
    Ddl.Builder ddlBuilder = Ddl.builder(Dialect.POSTGRESQL);
    List<Export.DatabaseOption> dbOptionList = new ArrayList<>();
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("default_sequence_kind")
            .setOptionValue("\"bit_reversed_positive\"")
            .build());
    ddlBuilder.mergeDatabaseOptions(dbOptionList);
    Ddl ddl = ddlBuilder
              .createTable("CommitTimestampTable")
              .column("id")
              .int64()
              .endColumn()
              .column("default_commit_ts")
              .pgSpannerCommitTimestamp()
              .defaultExpression("spanner.pending_commit_timestamp()")
              .endColumn()
              .column("on_update_ts")
              .pgSpannerCommitTimestamp()
              .defaultExpression("spanner.pending_commit_timestamp()")
              .onUpdateExpression("spanner.pending_commit_timestamp()")
              .endColumn()
              .primaryKey().asc("id").end()
              .endTable()
              .build();
    // spotless:on

    createAndPopulate(ddl, 10);
    runTest();
  }
  @Test
  public void udfs() throws Exception {
    Ddl.Builder ddlBuilder = Ddl.builder();
    List<Export.DatabaseOption> dbOptionList = new ArrayList<>();
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("default_sequence_kind")
            .setOptionValue("\"bit_reversed_positive\"")
            .build());
    ddlBuilder.mergeDatabaseOptions(dbOptionList);
    Ddl ddl =
        ddlBuilder
            .createSchema("s1")
            .endNamedSchema()
            .createUdf("s1.Foo1")
            .dialect(Dialect.GOOGLE_STANDARD_SQL)
            .name("s1.Foo1")
            .definition("(SELECT 'bar')")
            .endUdf()
            .createUdf("s1.Foo2")
            .dialect(Dialect.GOOGLE_STANDARD_SQL)
            .name("s1.Foo2")
            .definition("(SELECT 'bar')")
            .security(SqlSecurity.INVOKER)
            .type("STRING")
            .addParameter(UdfParameter.parse("arg0 STRING", "s1.Foo2", Dialect.GOOGLE_STANDARD_SQL))
            .addParameter(
                UdfParameter.parse(
                    "arg1 STRING DEFAULT 'bar'", "s1.Foo2", Dialect.GOOGLE_STANDARD_SQL))
            .endUdf()
            .build();
    createAndPopulate(ddl, 0);
    runTest();
  }
  @Test
  public void sequences() throws Exception {
    Ddl.Builder ddlBuilder = Ddl.builder();
    List<Export.DatabaseOption> dbOptionList = new ArrayList<>();
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("default_sequence_kind")
            .setOptionValue("\"bit_reversed_positive\"")
            .build());
    ddlBuilder.mergeDatabaseOptions(dbOptionList);
    Ddl ddl =
        ddlBuilder
            .createSequence("Sequence1")
            .options(
                ImmutableList.of(
                    "sequence_kind=\"bit_reversed_positive\"",
                    "skip_range_min=0",
                    "skip_range_max=1000",
                    "start_with_counter=50"))
            .endSequence()
            .createSequence("Sequence2")
            .options(
                ImmutableList.of(
                    "sequence_kind=\"bit_reversed_positive\"", "start_with_counter=9999"))
            .endSequence()
            .createSequence("Sequence3")
            .options(ImmutableList.of("sequence_kind=\"bit_reversed_positive\""))
            .endSequence()
            .createSequence("Sequence4")
            .options(
                ImmutableList.of(
                    "sequence_kind=\"default\"",
                    "skip_range_min=0",
                    "skip_range_max=1000",
                    "start_with_counter=50"))
            .endSequence()
            .createTable("UsersWithSequenceId")
            .column("id")
            .int64()
            .notNull()
            .defaultExpression("GET_NEXT_SEQUENCE_VALUE(SEQUENCE Sequence3)")
            .endColumn()
            .column("first_name")
            .string()
            .size(10)
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    createAndPopulate(ddl, 0);
    runTest();
  }
  @Test
  public void pgSequences() throws Exception {
    Ddl.Builder ddlBuilder = Ddl.builder(Dialect.POSTGRESQL);
    List<Export.DatabaseOption> dbOptionList = new ArrayList<>();
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("default_sequence_kind")
            .setOptionValue("\"bit_reversed_positive\"")
            .build());
    ddlBuilder.mergeDatabaseOptions(dbOptionList);
    Ddl ddl =
        ddlBuilder
            .createSequence("PGSequence1")
            .sequenceKind("bit_reversed_positive")
            .counterStartValue(Long.valueOf(50))
            .skipRangeMin(Long.valueOf(0))
            .skipRangeMax(Long.valueOf(1000))
            .endSequence()
            .createSequence("PGSequence2")
            .sequenceKind("bit_reversed_positive")
            .counterStartValue(Long.valueOf(9999))
            .endSequence()
            .createSequence("PGSequence3")
            .sequenceKind("bit_reversed_positive")
            .endSequence()
            .createSequence("PGSequence4")
            .sequenceKind("default")
            .counterStartValue(Long.valueOf(50))
            .skipRangeMin(Long.valueOf(0))
            .skipRangeMax(Long.valueOf(1000))
            .endSequence()
            .createTable("PGUsersWithSequenceId")
            .column("id")
            .pgInt8()
            .notNull()
            .defaultExpression("nextval('\"PGSequence3\"')")
            .endColumn()
            .column("first_name")
            .pgVarchar()
            .size(10)
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    createAndPopulate(ddl, 0);
    runTest(Dialect.POSTGRESQL);
  }
  @Test
  public void randomPgSchema() throws Exception {
    Ddl ddl = RandomDdlGenerator.builder(Dialect.POSTGRESQL).setMaxViews(2).build().generate();
    System.out.println(ddl.prettyPrint());
    createAndPopulate(ddl, 100);
    runTest(Dialect.POSTGRESQL);
  }
  @Test
  public void randomPgSchemaNoData() throws Exception {
    Ddl ddl = RandomDdlGenerator.builder(Dialect.POSTGRESQL).setMaxViews(2).build().generate();
    createAndPopulate(ddl, 0);
    runTest(Dialect.POSTGRESQL);
  }
}
