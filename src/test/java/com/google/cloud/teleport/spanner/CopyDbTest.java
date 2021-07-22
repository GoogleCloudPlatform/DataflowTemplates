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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.teleport.spanner.ExportProtos.Export;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.spanner.ddl.RandomDdlGenerator;
import com.google.cloud.teleport.spanner.ddl.Table;
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
 * identical. Additionally, this test verifies the behavior of table level export.
 * This requires an active GCP project with a Spanner instance. Hence this test can only be run
 * locally with a project set up using 'gcloud config'.
 */
@Category(IntegrationTest.class)
public class CopyDbTest {
  private final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
  private final long numericTime = timestamp.getTime();
  private final String sourceDb = "copydb-source" + Long.toString(numericTime);
  private final String destinationDb = "copydb-dest" + Long.toString(numericTime);
  private final String destDbPrefix = "import";
  private final String usersTable = "Users";
  private final String allTypesTable = "AllTYPES";
  private final String peopleTable = "People";
  private final String emptyTable = "empty_table";
  private final String fullExportChkpt = "fullexportchkpt";
  private final String usersChkpt = "userschkpt";
  private final String multiTableChkpt = "multichkpt";
  private final String emptyChkpt = "emptychkpt";

  @Rule public final transient TestPipeline exportPipeline = TestPipeline.create();
  @Rule public final transient TestPipeline importPipeline = TestPipeline.create();
  @Rule public final transient TestPipeline comparePipeline = TestPipeline.create();
  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();
  @Rule public final SpannerServerResource spannerServer = new SpannerServerResource();

  @After
  public void teardown() {
    spannerServer.dropDatabase(sourceDb);
    spannerServer.dropDatabase(destinationDb);
    spannerServer.dropDatabase(destDbPrefix + fullExportChkpt);
    spannerServer.dropDatabase(destDbPrefix + usersChkpt);
    spannerServer.dropDatabase(destDbPrefix + multiTableChkpt);
    spannerServer.dropDatabase(destDbPrefix + emptyChkpt);
  }

  private void createAndPopulate(Ddl ddl, int numBatches) throws Exception {
    spannerServer.createDatabase(sourceDb, ddl.statements());
    spannerServer.createDatabase(destinationDb, Collections.emptyList());
    spannerServer.populateRandomData(sourceDb, ddl, numBatches);
  }

  /* Validates behavior of exporting full db without selecting any tables */
  @Test
  public void exportWithoutTableSelection() throws Exception {
    Ddl ddl = Ddl.builder()
            .createTable("Users")
              .column("first_name").string().max().endColumn()
              .column("last_name").string().size(5).endColumn()
              .column("age").int64().endColumn()
              .primaryKey().asc("first_name").desc("last_name").end()
            .endTable()
            .createTable("People")
              .column("id").int64().notNull().endColumn()
              .column("name").string().max().endColumn()
              .column("age").int64().endColumn()
              .primaryKey().asc("id").end()
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
            .endTable()
            .build();

    createAndPopulate(ddl, 100);

    // Export and import all tables from the database
    spannerServer.createDatabase(destDbPrefix + fullExportChkpt, Collections.emptyList());
    exportAndImportDb(
        sourceDb,
        destDbPrefix + fullExportChkpt,
        fullExportChkpt,
        "",
        exportPipeline,
        importPipeline);

    // Compare the tables in the ddl to ensure all original tables were re-created during the import
    compareExpectedTables(
        destDbPrefix + fullExportChkpt, ImmutableList.of(allTypesTable, peopleTable, usersTable));

    // Check to see all tables exported with their original data
    assertFalse(getRowCount(destDbPrefix + fullExportChkpt, peopleTable) == 0);

    assertFalse(getRowCount(destDbPrefix + fullExportChkpt, allTypesTable) == 0);

    assertFalse(getRowCount(destDbPrefix + fullExportChkpt, usersTable) == 0);
  }

  /* Validates behavior of single table database exporting */
  @Test
  public void exportSingleTable() throws Exception {
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
            .endTable()
            .build();

    createAndPopulate(ddl, 100);
    // Export and import the table 'Users' from the database only
    spannerServer.createDatabase(destDbPrefix + usersChkpt, Collections.emptyList());
    exportAndImportDb(
        sourceDb,
        destDbPrefix + usersChkpt,
        usersChkpt,
        usersTable,
        exportPipeline,
        importPipeline);

    // Compare the tables in the ddl to ensure all original tables were re-created during the import
    compareExpectedTables(destDbPrefix + usersChkpt, ImmutableList.of(allTypesTable, usersTable));

    // Check to see table `AllTYPES` exported without data
    assertEquals(0, getRowCount(destDbPrefix + usersChkpt, allTypesTable));

    // Check to see `Users` table exported with its original data
    assertFalse(getRowCount(destDbPrefix + usersChkpt, usersTable) == 0);
  }

  /* Validates behavior of exporting multiple unrelated tables */
  @Test
  public void exportMultipleTables() throws Exception {
    Ddl ddl = Ddl.builder()
            .createTable("Users")
              .column("first_name").string().max().endColumn()
              .column("last_name").string().size(5).endColumn()
              .column("age").int64().endColumn()
              .primaryKey().asc("first_name").desc("last_name").end()
            .endTable()
            .createTable("People")
              .column("id").int64().notNull().endColumn()
              .column("name").string().max().endColumn()
              .column("age").int64().endColumn()
              .primaryKey().asc("id").end()
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
            .endTable()
            .build();

    createAndPopulate(ddl, 100);

    // Export and import two specific tables from the database containing three tables
    spannerServer.createDatabase(destDbPrefix + multiTableChkpt, Collections.emptyList());
    exportAndImportDb(
        sourceDb,
        destDbPrefix + multiTableChkpt,
        multiTableChkpt,
        usersTable + "," + allTypesTable,
        exportPipeline,
        importPipeline);

    // Compare the tables in the ddl to ensure all original tables were re-created during the import
    compareExpectedTables(
        destDbPrefix + multiTableChkpt, ImmutableList.of(allTypesTable, peopleTable, usersTable));

    // Check to see table `People` exported without data
    assertEquals(0, getRowCount(destDbPrefix + multiTableChkpt, peopleTable));

    // Check to see `ALLTYPES` and `Users` tables exported with their original data
    assertFalse(getRowCount(destDbPrefix + multiTableChkpt, allTypesTable) == 0);

    assertFalse(getRowCount(destDbPrefix + multiTableChkpt, usersTable) == 0);
  }

  /* Validates behavior of exporting a single, empty table from a database */
  @Test
  public void exportSingleEmptyTable() throws Exception {
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
            .endTable()
            .build();

    createAndPopulate(ddl, 100);

    // Add empty table.
    Ddl ddlEmptyTable = Ddl.builder()
        .createTable("empty_table")
          .column("first").string().max().endColumn()
          .column("second").string().size(5).endColumn()
          .column("value").int64().endColumn()
          .primaryKey().asc("first").desc("second").end()
          .endTable()
        .build();
    spannerServer.updateDatabase(sourceDb, ddlEmptyTable.createTableStatements());

    // Export an empty table from a database
    spannerServer.createDatabase(destDbPrefix + emptyChkpt, Collections.emptyList());
    exportAndImportDb(
        sourceDb,
        destDbPrefix + emptyChkpt,
        emptyChkpt,
        emptyTable,
        exportPipeline,
        importPipeline);

    // Compare the tables in the ddl to ensure all original tables were re-created during the import
    compareExpectedTables(
        destDbPrefix + emptyChkpt, ImmutableList.of(allTypesTable, usersTable, emptyTable));

    // Check to see `ALLTYPES` and `Users` tables exported without data
    assertEquals(0, getRowCount(destDbPrefix + emptyChkpt, allTypesTable));

    assertEquals(0, getRowCount(destDbPrefix + emptyChkpt, usersTable));

    // Check to see `empty_table` table exported and it's still empty
    assertEquals(0, getRowCount(destDbPrefix + emptyChkpt, emptyTable));
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
  public void databaseOptions() throws Exception {
    Ddl.Builder ddlBuilder = Ddl.builder();
    // Table Content
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

  private void exportAndImportDb(String sourceDb, String destDb,
                                       String jobIdName, String tableNames,
                                       TestPipeline exportPipeline,
                                       TestPipeline importPipeline) {
    String tmpDirPath = tmpDir.getRoot().getAbsolutePath();
    ValueProvider.StaticValueProvider<String> destination = ValueProvider.StaticValueProvider
        .of(tmpDirPath);
    ValueProvider.StaticValueProvider<String> jobId = ValueProvider.StaticValueProvider
        .of(jobIdName);
    ValueProvider.StaticValueProvider<String> source = ValueProvider.StaticValueProvider
        .of(tmpDirPath + "/" + jobIdName);
    ValueProvider.StaticValueProvider<String> timestamp = ValueProvider.StaticValueProvider.of("");
    ValueProvider.StaticValueProvider<String> tables = ValueProvider.StaticValueProvider
        .of(tableNames);
    ValueProvider.StaticValueProvider<Boolean> exportAsLogicalType =
        ValueProvider.StaticValueProvider.of(false);
    SpannerConfig sourceConfig = spannerServer.getSpannerConfig(sourceDb);
    exportPipeline.apply("Export", new ExportTransform(sourceConfig, destination,
                                                       jobId, timestamp, tables,
                                                       exportAsLogicalType));
    PipelineResult exportResult = exportPipeline.run();
    exportResult.waitUntilFinish();

    SpannerConfig copyConfig = spannerServer.getSpannerConfig(destDb);
    importPipeline.apply("Import", new ImportTransform(
        copyConfig, source, ValueProvider.StaticValueProvider.of(true),
        ValueProvider.StaticValueProvider.of(true),
        ValueProvider.StaticValueProvider.of(true)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();
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

    assertThat(sourceDdl.prettyPrint(), equalToCompressingWhiteSpace(destinationDdl.prettyPrint()));
  }

  /* Compares a given List of strings for the expected tables in a database (in alphabetical order)
   * and compares that list to the list of actual tables in that database */
  private void compareExpectedTables(String sourceDb, List<String> expectedTables) {
    Ddl sourceDdl = readDdl(sourceDb);

    List<String> tables = getTableNamesFromDdl(sourceDdl);
    Collections.sort(tables);

    assertEquals(expectedTables, tables);
  }

  /* Returns a List of strings for each Table name from a Ddl */
  private List<String> getTableNamesFromDdl(Ddl ddl) {
    List<String> tableNames = Lists.newArrayList();

    for (Table t : ddl.allTables()) {
       tableNames.add(t.name());
    }
    return tableNames;
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

  /* Given a database and table name, query that table and return the number of rows it has */
  private long getRowCount(String db, String tableName) {
    DatabaseClient dbClient = spannerServer.getDbClient(db);
    ReadOnlyTransaction context = dbClient.readOnlyTransaction();
    // Execute query to determine how many rows are in the table
    ResultSet resultSet =
      context.executeQuery(
          Statement.of(
              String.format("SELECT COUNT(1) FROM `%s`", tableName)));
    while (resultSet.next()) {
      long rows = resultSet.getLong(0);
      return rows;
    }
    return -1;
  }
}
