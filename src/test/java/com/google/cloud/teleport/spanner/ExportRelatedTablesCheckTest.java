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

    // Check to see all tables exported with their original data
    assertFalse(getRowCount(destDbPrefix + chkptOne, tableA) == 0);

    assertFalse(getRowCount(destDbPrefix + chkptOne, tableB) == 0);

    assertFalse(getRowCount(destDbPrefix + chkptOne, tableC) == 0);
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

    // Check to see table_a and table_b exported without data
    assertEquals(0, getRowCount(destDbPrefix + chkptTwo, tableA));
    assertEquals(0, getRowCount(destDbPrefix + chkptTwo, tableB));

    // Check to see table_c exported with its original data
    assertFalse(getRowCount(destDbPrefix + chkptTwo, tableC) == 0);
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

    // Check to see all tables exported with their original data
    assertFalse(getRowCount(destDbPrefix + chkptFive, tableA) == 0);

    assertFalse(getRowCount(destDbPrefix + chkptFive, tableB) == 0);

    assertFalse(getRowCount(destDbPrefix + chkptFive, tableC) == 0);
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

    // Check to table_b, and table_c were exported with their original data
    assertFalse(getRowCount(destDbPrefix + chkptSix, tableB) == 0);

    assertFalse(getRowCount(destDbPrefix + chkptSix, tableC) == 0);

    // Check to see table_a table_d exported without data
    assertEquals(0, getRowCount(destDbPrefix + chkptSix, tableA));
    assertEquals(0, getRowCount(destDbPrefix + chkptSix, tableD));
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

    // Check to see that every randomly chosen table was exported with data
    for (String exortTable : filteredTables) {
      assertFalse(getRowCount(destDbPrefix + chkptSeven, exortTable) == 0);
    }

    // Check to see that every unselected table was exported without data
    for (String table : tableNames) {
      if (!(filteredTables.contains(table))) {
        assertEquals(0, getRowCount(destDbPrefix + chkptSeven, table));
      }
    }
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

    // Check to see expected tables were exported with their original data
    assertFalse(getRowCount(destDbPrefix + chkptNine, tableA) == 0);
    assertFalse(getRowCount(destDbPrefix + chkptNine, tableB) == 0);
    assertFalse(getRowCount(destDbPrefix + chkptNine, tableC) == 0);
    assertFalse(getRowCount(destDbPrefix + chkptNine, tableE) == 0);
    assertFalse(getRowCount(destDbPrefix + chkptNine, tableF) == 0);
    assertFalse(getRowCount(destDbPrefix + chkptNine, tableH) == 0);
    assertFalse(getRowCount(destDbPrefix + chkptNine, tableI) == 0);
    assertFalse(getRowCount(destDbPrefix + chkptNine, tableJ) == 0);

    // Check to see expected tables exported without data
    assertEquals(0, getRowCount(destDbPrefix + chkptNine, tableD));
    assertEquals(0, getRowCount(destDbPrefix + chkptNine, tableK));
    assertEquals(0, getRowCount(destDbPrefix + chkptNine, tableL));
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
            ValueProvider.StaticValueProvider.of(true)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();
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
