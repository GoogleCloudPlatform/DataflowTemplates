/*
 * Copyright (C) 2021 Google LLC
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

import static com.google.cloud.teleport.spanner.SpannerTableFilter.getFilteredTables;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.spanner.ddl.RandomDdlGenerator;
import com.google.cloud.teleport.spanner.ddl.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * An end to end test that exports and imports a database and verifies that the content is
 * identical. Additionally, this test verifies the behavior of --tableNames and
 * --shouldExportRelatedTables parameters. This requires an active GCP project with a Spanner
 * instance. Hence this test can only be run locally with a project set up using 'gcloud config'.
 */
@Category(IntegrationTest.class)
public final class ExportRelatedTablesCheckTest {
  private final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
  private final long numericTime = timestamp.getTime();
  private final String sourceDb = "flagdb-source" + Long.toString(numericTime);
  private final String destinationDb = "flagdb-dest" + Long.toString(numericTime);
  private final String destDbPrefix = "import";
  private final String usersTable = "Users";
  private final String allTypesTable = "AllTYPES";
  private final String peopleTable = "People";
  private final String emptyTable = "empty_table";
  private final String fullExportChkpt = "fullexportchkpt";
  private final String usersChkpt = "userschkpt";
  private final String multiTableChkpt = "multichkpt";
  private final String emptyChkpt = "emptychkpt";
  private final String tableA = "table_a";
  private final String tableB = "table_b";
  private final String tableC = "table_c";
  private final String tableD = "table_d";
  private final String tableE = "table_e";
  private final String tableF = "table_f";
  private final String tableG = "table_g";
  private final String tableH = "table_h";
  private final String tableI = "table_i";
  private final String tableJ = "table_j";
  private final String tableK = "table_k";
  private final String tableL = "table_l";
  private final String chkptOne = "chkpt1";
  private final String chkptTwo = "chkpt2";
  private final String chkptThree = "chkpt3";
  private final String chkptFour = "chkpt4";
  private final String chkptFive = "chkpt5";
  private final String chkptSix = "chkpt6";
  private final String chkptSeven = "chkpt7";
  private final String chkptEight = "chkpt8";
  private final String chkptNine = "chkpt9";

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
    spannerServer.dropDatabase(destDbPrefix + chkptOne);
    spannerServer.dropDatabase(destDbPrefix + chkptTwo);
    spannerServer.dropDatabase(destDbPrefix + chkptThree);
    spannerServer.dropDatabase(destDbPrefix + chkptFour);
    spannerServer.dropDatabase(destDbPrefix + chkptFive);
    spannerServer.dropDatabase(destDbPrefix + chkptSix);
    spannerServer.dropDatabase(destDbPrefix + chkptSeven);
    spannerServer.dropDatabase(destDbPrefix + chkptEight);
    spannerServer.dropDatabase(destDbPrefix + chkptNine);
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
        /* relatedTables =*/ false,
        exportPipeline,
        importPipeline);

    // Compare the tables in the ddl to ensure all original tables were re-created during the import
    compareExpectedTables(
        destDbPrefix + fullExportChkpt, ImmutableList.of(allTypesTable, peopleTable, usersTable));


    // Check to see selected tables exported with data and and unselected tables did not
    List<String> exportTables = ImmutableList.of(allTypesTable, peopleTable, usersTable);
    List<String> unselectedTables = Collections.emptyList();
    compareExpectedTableRows(destDbPrefix + fullExportChkpt, exportTables, unselectedTables);
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
        /* relatedTables =*/ false,
        exportPipeline,
        importPipeline);

    // Compare the tables in the ddl to ensure all original tables were re-created during the import
    compareExpectedTables(destDbPrefix + usersChkpt, ImmutableList.of(allTypesTable, usersTable));

    // Check to see selected tables exported with data and and unselected tables did not
    List<String> exportTables = ImmutableList.of(usersTable);
    List<String> unselectedTables = ImmutableList.of(allTypesTable);
    compareExpectedTableRows(destDbPrefix + usersChkpt, exportTables, unselectedTables);
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
        /* relatedTables =*/ false,
        exportPipeline,
        importPipeline);

    // Compare the tables in the ddl to ensure all original tables were re-created during the import
    compareExpectedTables(
        destDbPrefix + multiTableChkpt, ImmutableList.of(allTypesTable, peopleTable, usersTable));

    // Check to see selected tables exported with data and and unselected tables did not
    List<String> exportTables = ImmutableList.of(allTypesTable, usersTable);
    List<String> unselectedTables = ImmutableList.of(peopleTable);
    compareExpectedTableRows(destDbPrefix + multiTableChkpt, exportTables, unselectedTables);
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
        /* relatedTables =*/ false,
        exportPipeline,
        importPipeline);

    // Compare the tables in the ddl to ensure all original tables were re-created during the import
    compareExpectedTables(
        destDbPrefix + emptyChkpt, ImmutableList.of(allTypesTable, usersTable, emptyTable));

    // Check to see selected tables exported with data and and unselected tables did not
    List<String> exportTables = Collections.emptyList();
    List<String> unselectedTables = ImmutableList.of(allTypesTable, usersTable, emptyTable);
    compareExpectedTableRows(destDbPrefix + emptyChkpt, exportTables, unselectedTables);
  }

  /* Validates that pipeline executes full-db export when --tableNames and
   * --shouldExportRelatedTables paramters are not filled */
  @Test
  public void exportDbWithoutTableNamesAndFlag_exportsFullDb() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("table_a")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .end()
            .endTable()
            .createTable("table_b")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("id3")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .endTable()
            .createTable("table_c")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("id3")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .interleaveInParent("table_b")
            .endTable()
            .build();

    // Add to referencedTable field (i.e. `table_c` would have a foreign key constraint
    // referencing `table_a` )
    ddl.addNewReferencedTable("table_c", "table_a");

    createAndPopulate(ddl, /* numBatches = */ 100);

    // Export the entire database (without setting --tablesName or --shouldExportRelatedTables)
    spannerServer.createDatabase(destDbPrefix + chkptOne, Collections.emptyList());
    exportAndImportDb(
        sourceDb,
        destDbPrefix + chkptOne,
        chkptOne,
        "", // --tableNames would not be set, defaults to an empty string
        /* relatedTables =*/ false,
        exportPipeline,
        importPipeline);

    // Compare the tables in the ddl to ensure all original tables were re-created during the import
    compareExpectedTables(destDbPrefix + chkptOne, ImmutableList.of(tableA, tableB, tableC));

    // Check to see selected tables exported with data and and unselected tables did not
    List<String> exportTables = ImmutableList.of(tableA, tableB, tableC);
    List<String> unselectedTables = Collections.emptyList();
    compareExpectedTableRows(destDbPrefix + chkptOne, exportTables, unselectedTables);
  }

  /* Validates that pipeline exports single table that has no related tables when
   * --shouldExportRelatedTables parameter is not filled */
  @Test
  public void exportTableWithoutRelatedTablesAndWithoutFlag_exportsSelectedTable()
      throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("table_a")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .end()
            .endTable()
            .createTable("table_b")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("id3")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .interleaveInParent("table_a")
            .endTable()
            .createTable("table_c")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("id3")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .endTable()
            .build();

    createAndPopulate(ddl, /* numBatches = */ 100);

    // Export the single disconnected table database (without --shouldExportRelatedTables)
    spannerServer.createDatabase(destDbPrefix + chkptTwo, Collections.emptyList());
    exportAndImportDb(
        sourceDb,
        destDbPrefix + chkptTwo,
        chkptTwo,
        tableC,
        /* relatedTables =*/ false,
        exportPipeline,
        importPipeline);

    // Compare the tables in the ddl to ensure all original tables were re-created during the import
    compareExpectedTables(destDbPrefix + chkptTwo, ImmutableList.of(tableA, tableB, tableC));

    // Check to see selected tables exported with data and and unselected tables did not
    List<String> exportTables = ImmutableList.of(tableC);
    List<String> unselectedTables = ImmutableList.of(tableA, tableB);
    compareExpectedTableRows(destDbPrefix + chkptTwo, exportTables, unselectedTables);
  }

  /* Validates that pipeline execution fails when --tableNames is provided and
   * --shouldExportRelatedTables is not filled/set to false, but additional tables
   * need to be exported */
  @Test
  public void exportTableWithRelatedTablesAndWithoutFlag_stopsPipelineExecution() throws Exception {
    Ddl ddl = Ddl.builder()
        .createTable("table_a")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").end()
        .endTable()
        .createTable("table_b")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
        .endTable()
        .createTable("table_c")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
          .interleaveInParent("table_b")
          .foreignKeys(
                ImmutableList.of(
                    "ALTER TABLE `table_c` ADD CONSTRAINT `fk_table_b` FOREIGN KEY (`id1`)"
                        + " REFERENCES `table_b` (`id1`)"))
        .endTable()
        .createTable("table_d")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
        .endTable()
        .createTable("table_e")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
        .endTable()
        .createTable("table_f")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
          .interleaveInParent("table_e")
          .foreignKeys(
                ImmutableList.of(
                    "ALTER TABLE `table_f` ADD CONSTRAINT `fk_table_f` FOREIGN KEY (`id2`)"
                        + " REFERENCES `table_e` (`id2`)"))
        .endTable()
        .createTable("table_g")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
        .endTable()
        .createTable("table_h")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
        .endTable()
        .createTable("table_i")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
          .interleaveInParent("table_h")
        .endTable()
        .createTable("table_j")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
          .interleaveInParent("table_i")
        .endTable()
        .createTable("table_k")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
        .endTable()
        .createTable("table_l")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
        .endTable()
        .build();

    createAndPopulate(ddl, /* numBatches = */ 100);

    // Expected PipelineExecutionException caused by Exception:
    // Attempt to export a single table that requires additional related tables
    // (without --shouldExportRelatedTables set/setting --shouldExportRelatedTables true)
    spannerServer.createDatabase(destDbPrefix + chkptThree, Collections.emptyList());
    Exception exception = assertThrows(
        PipelineExecutionException.class,
        () ->
            exportAndImportDb(
                sourceDb,
                destDbPrefix + chkptThree,
                chkptThree,
                String.join(",", ImmutableList.of(tableA, tableC, tableF, tableJ)),
                /* relatedTables =*/ false,
                exportPipeline,
                importPipeline));

    List<String> missingTables = ImmutableList.of(tableB, tableE, tableH, tableI);
    assertEquals(
        "java.lang.Exception: Attempted to export table(s) requiring parent and/or foreign keys"
            + " tables without setting the shouldExportRelatedTables parameter. Set"
            + " --shouldExportRelatedTables=true to export all necessary tables, or add "
            + String.join(", ", missingTables)
            + " to --tableNames.",
        exception.getMessage());
  }

  /* Validates that pipeline execution fails when --tableNames is not filled and
   * --shouldExportRelatedTables is set to true */
  @Test
  public void exportFullDbWithFlagTrue() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("table_a")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .end()
            .endTable()
            .createTable("table_b")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("id3")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .endTable()
            .createTable("table_c")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("id3")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .interleaveInParent("table_b")
            .endTable()
            .build();

    // Add to referencedTable field (i.e. `table_c` would have a foreign key constraint
    // referencing `table_a` )
    ddl.addNewReferencedTable("table_c", "table_a");

    createAndPopulate(ddl, /* numBatches = */ 100);

    // Expected PipelineExecutionException caused by Exception:
    // set --shouldExportRelatedTables to true when no tables were specified for export
    spannerServer.createDatabase(destDbPrefix + chkptFour, Collections.emptyList());
    assertThrows(
        PipelineExecutionException.class,
        () ->
            exportAndImportDb(
                sourceDb,
                destDbPrefix + chkptFour,
                chkptFour,
                "", // --tableNames would not be set, defaults to an empty string
                /* relatedTables =*/ true,
                exportPipeline,
                importPipeline));
  }

  /* Validates that pipeline executes full-db export when --tableNames is not filled and
   * --shouldExportRelatedTables is set to false (either intentionally or by default) */
  @Test
  public void exportFullDbWithFlagFalse() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("table_a")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .end()
            .endTable()
            .createTable("table_b")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("id3")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .endTable()
            .createTable("table_c")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("id3")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .interleaveInParent("table_b")
            .endTable()
            .build();

    // Add to referencedTable field (i.e. `table_c` would have a foreign key constraint
    // referencing `table_a` )
    ddl.addNewReferencedTable("table_c", "table_a");

    createAndPopulate(ddl, /* numBatches = */ 100);

    // Export the entire database (without setting --tablesName)
    spannerServer.createDatabase(destDbPrefix + chkptFive, Collections.emptyList());
    exportAndImportDb(
        sourceDb,
        destDbPrefix + chkptFive,
        chkptFive,
        "", // --tableNames would not be set, defaults to an empty string
        /* relatedTables =*/ false,
        exportPipeline,
        importPipeline);

    // Compare the tables in the ddl to ensure all original tables were re-created during the import
    compareExpectedTables(destDbPrefix + chkptFive, ImmutableList.of(tableA, tableB, tableC));

    // Check to see selected tables exported with data and and unselected tables did not
    List<String> exportTables = ImmutableList.of(tableA, tableB, tableC);
    List<String> unselectedTables = Collections.emptyList();
    compareExpectedTableRows(destDbPrefix + chkptFive, exportTables, unselectedTables);
  }

  /* Validates that pipeline executes table level export when --tableNames is provided,
   * --shouldExportRelatedTables is set to true, and additional tables need to be exported */
  @Test
  public void exportSelectedAndNecessaryTables() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("table_a")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .end()
            .endTable()
            .createTable("table_b")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("id3")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .endTable()
            .createTable("table_c")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("id3")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .interleaveInParent("table_b")
            .foreignKeys(
                ImmutableList.of(
                    "ALTER TABLE `table_c` ADD CONSTRAINT `fk1` FOREIGN KEY (`id1`) REFERENCES"
                        + " `table_b` (`id1`)"))
            .endTable()
            .createTable("table_d")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("id3")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .endTable()
            .build();

    createAndPopulate(ddl, /* numBatches = */ 100);

    // Export the single table along with it's required tables
    spannerServer.createDatabase(destDbPrefix + chkptSix, Collections.emptyList());
    exportAndImportDb(
        sourceDb,
        destDbPrefix + chkptSix,
        chkptSix,
        tableC, // --tableNames would be given tableC
        /* relatedTables =*/ true,
        exportPipeline,
        importPipeline);

    // Compare the tables in the ddl to ensure all original tables were re-created during the import
    compareExpectedTables(
        destDbPrefix + chkptSix, ImmutableList.of(tableA, tableB, tableC, tableD));

    // Check to see selected tables exported with data and and unselected tables did not
    List<String> exportTables = ImmutableList.of(tableB, tableC);
    List<String> unselectedTables = ImmutableList.of(tableA, tableD);
    compareExpectedTableRows(destDbPrefix + chkptSix, exportTables, unselectedTables);
  }

  /* Validates behavior of table filtering using randomly generated table and randomly selected
   * export tables */
  @Test
  public void randomExportTest() throws Exception {
    Ddl ddl = RandomDdlGenerator.builder().build().generate();
    createAndPopulate(ddl, /* numBatches = */ 100);

    // Print the Ddl
    ddl.prettyPrint();

    // Get all the tables names from the random Ddl
    List<String> tableNames =
        ddl.allTables().stream().map(t -> t.name()).collect(Collectors.toList());

    // Select a random number of random tables from the Ddl to export
    Random rand = new Random();
    Collections.shuffle(tableNames, rand);

    int n = rand.nextInt(tableNames.size());
    // Ensure n != 0, so that tables are actually exported
    while (n == 0) {
      n = rand.nextInt(tableNames.size());
    }

    List<String> randomExportTables = tableNames.stream().limit(n).collect(Collectors.toList());

    // Export and import the selected tables, along with any necessary related tables
    spannerServer.createDatabase(destDbPrefix + chkptSeven, Collections.emptyList());
    exportAndImportDb(
        sourceDb,
        destDbPrefix + chkptSeven,
        chkptSeven,
        String.join(",", randomExportTables),
        /* relatedTables =*/ true,
        exportPipeline,
        importPipeline);

    // Compare the tables in the ddl to ensure all original tables were re-created during the import
    Collections.sort(tableNames);
    compareExpectedTables(destDbPrefix + chkptSeven, tableNames);

    // Get ALL the exported tables by calling the getFilteredTables() helper
    List<String> filteredTables =
        getFilteredTables(ddl, randomExportTables).stream()
            .map(t -> t.name())
            .collect(Collectors.toList());

    List<String> unselectedTables =
        tableNames.stream()
            .distinct()
            .filter(t -> !filteredTables.contains(t))
            .collect(Collectors.toList());

    // Check to see selected tables exported with data and and unselected tables did not
    compareExpectedTableRows(destDbPrefix + chkptSeven, filteredTables, unselectedTables);
  }

  /* Validates that pipeline execution fails when --tableNames is given a table that doesn't
   * exist in the database */
  @Test
  public void exportNonExistentTable_stopsPipelineExecution() throws Exception {
    Ddl ddl = Ddl.builder()
        .createTable("table_a")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").end()
        .endTable()
        .createTable("table_b")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
        .endTable()
        .createTable("table_c")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
          .interleaveInParent("table_b")
        .endTable()
        .build();

    // Add to referencedTable field (i.e. `table_c` would have a foreign key constraint
    // referencing `table_a`)
    ddl.addNewReferencedTable("table_c", "table_a");

    createAndPopulate(ddl, /* numBatches = */ 100);

    // Expected PipelineExecutionException caused by Exception:
    // Attempt to export a non existent table 'table_d'.
    spannerServer.createDatabase(destDbPrefix + chkptEight, Collections.emptyList());
    assertThrows(
        PipelineExecutionException.class,
        () ->
            exportAndImportDb(
                sourceDb,
                destDbPrefix + chkptEight,
                chkptEight,
                tableD,
                /* relatedTables =*/ false,
                exportPipeline,
                importPipeline));
  }

  /* Validates that pipeline executes table level export for this complex ddl when --tableNames
   * is provided, --shouldExportRelatedTables is set to true, and additional tables need
   * to be exported. */
  @Test
  public void exportSelectedAndNecessaryTablesInComplexDdl() throws Exception {
    Ddl ddl = Ddl.builder()
        .createTable("table_a")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").end()
        .endTable()
        .createTable("table_b")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
        .endTable()
        .createTable("table_c")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
          .interleaveInParent("table_b")
          .foreignKeys(
                ImmutableList.of(
                    "ALTER TABLE `table_c` ADD CONSTRAINT `fk_table_b` FOREIGN KEY (`id1`)"
                        + " REFERENCES `table_b` (`id1`)"))
        .endTable()
        .createTable("table_d")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
        .endTable()
        .createTable("table_e")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
        .endTable()
        .createTable("table_f")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
          .interleaveInParent("table_e")
          .foreignKeys(
                ImmutableList.of(
                    "ALTER TABLE `table_f` ADD CONSTRAINT `fk_table_f` FOREIGN KEY (`id2`)"
                        + " REFERENCES `table_e` (`id2`)"))
        .endTable()
        .createTable("table_g")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
        .endTable()
        .createTable("table_h")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
        .endTable()
        .createTable("table_i")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
          .interleaveInParent("table_h")
        .endTable()
        .createTable("table_j")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
          .interleaveInParent("table_i")
        .endTable()
        .createTable("table_k")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
        .endTable()
        .createTable("table_l")
          .column("id1").int64().endColumn()
          .column("id2").int64().endColumn()
          .column("id3").int64().endColumn()
          .primaryKey().asc("id1").asc("id2").asc("id3").end()
        .endTable()
        .build();

    createAndPopulate(ddl, /* numBatches = */ 100);

    // Export the single table along with it's required tables
    spannerServer.createDatabase(destDbPrefix + chkptNine, Collections.emptyList());
    exportAndImportDb(
        sourceDb,
        destDbPrefix + chkptNine,
        chkptNine,
        String.join(",", ImmutableList.of(tableA, tableC, tableF, tableJ)),
        /* relatedTables =*/ true,
        exportPipeline,
        importPipeline);

    // Compare the tables in the ddl to ensure all original tables were re-created during the import
    compareExpectedTables(
        destDbPrefix + chkptNine,
        ImmutableList.of(
            tableA, tableB, tableC, tableD, tableE, tableF, tableG, tableH, tableI, tableJ, tableK,
            tableL));

    // Check to see selected tables exported with data and and unselected tables did not
    List<String> exportTables =
        ImmutableList.of(tableA, tableB, tableC, tableE, tableF, tableH, tableI, tableJ);
    List<String> unselectedTables = ImmutableList.of(tableD, tableK, tableL);
    compareExpectedTableRows(destDbPrefix + chkptNine, exportTables, unselectedTables);
  }

  private void exportAndImportDb(
      String sourceDb,
      String destDb,
      String jobIdName,
      String tableNames,
      Boolean relatedTables,
      TestPipeline exportPipeline,
      TestPipeline importPipeline) {
    String tmpDirPath = tmpDir.getRoot().getAbsolutePath();
    ValueProvider.StaticValueProvider<String> destination =
        ValueProvider.StaticValueProvider.of(tmpDirPath);
    ValueProvider.StaticValueProvider<String> jobId =
        ValueProvider.StaticValueProvider.of(jobIdName);
    ValueProvider.StaticValueProvider<String> source =
        ValueProvider.StaticValueProvider.of(tmpDirPath + "/" + jobIdName);
    ValueProvider.StaticValueProvider<String> timestamp = ValueProvider.StaticValueProvider.of("");
    ValueProvider.StaticValueProvider<String> tables =
        ValueProvider.StaticValueProvider.of(tableNames);
    ValueProvider.StaticValueProvider<Boolean> exportRelatedTables =
        ValueProvider.StaticValueProvider.of(relatedTables);
    ValueProvider.StaticValueProvider<Boolean> exportAsLogicalType =
        ValueProvider.StaticValueProvider.of(false);
    SpannerConfig sourceConfig = spannerServer.getSpannerConfig(sourceDb);
    exportPipeline.apply(
        "Export",
        new ExportTransform(
            sourceConfig,
            destination,
            jobId,
            timestamp,
            tables,
            exportRelatedTables,
            exportAsLogicalType));
    PipelineResult exportResult = exportPipeline.run();
    exportResult.waitUntilFinish();

    SpannerConfig copyConfig = spannerServer.getSpannerConfig(destDb);
    importPipeline.apply(
        "Import",
        new ImportTransform(
            copyConfig,
            source,
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(30)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();
  }

  /* Compares the tables in a database against their expected row counts */
  private void compareExpectedTableRows(
      String db, List<String> exportTables, List<String> unselectedTables) {
    // Check to see that every table that should be exported with data contains rows upon import
    for (String table : exportTables) {
      assertFalse(getRowCount(db, table) == 0);
    }

    // Check to see that every unselected table was exported without data
    for (String table : unselectedTables) {
      assertEquals(0, getRowCount(db, table));
    }
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
        context.executeQuery(Statement.of(String.format("SELECT COUNT(1) FROM `%s`", tableName)));
    while (resultSet.next()) {
      long rows = resultSet.getLong(0);
      return rows;
    }
    return -1;
  }
}
