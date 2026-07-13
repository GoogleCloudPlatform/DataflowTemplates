/*
 * Copyright (C) 2026 Google LLC
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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SpannerStagingTest;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.spanner.ddl.Column;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.spanner.ddl.RandomInsertMutationGenerator;
import com.google.cloud.teleport.spanner.ddl.Table;
import com.google.cloud.teleport.spanner.spannerio.MutationGroup;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerTemplateITBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration test for {@link ExportPipeline} and {@link ImportPipeline}.
 *
 * <p>This test completely validates the entire lifecycle of exporting a Spanner database to GCS
 * using Avro/JSON format, and subsequently importing that exact data into a fresh Spanner database.
 * It natively supports testing both Google Standard SQL (GSQL) and PostgreSQL dialects with various
 * complex schema combinations (e.g., interleaved tables, foreign keys, arrays) and random data.
 */
@Category({TemplateIntegrationTest.class, SpannerStagingTest.class})
@TemplateIntegrationTest(ExportPipeline.class)
public class CopyDbIT extends SpannerTemplateITBase {

  // Resource managers for the source database (exported) and destination database (imported).
  private SpannerResourceManager sourceResourceManager;
  private SpannerResourceManager destResourceManager;

  @Before
  public void setup() {
    // We intentionally do NOT initialize the SpannerResourceManagers here.
    // They are instantiated dynamically in the `createAndPopulate` method to
    // support setting the correct Spanner Dialect (GSQL vs PG) for each specific test case.
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(sourceResourceManager, destResourceManager);
  }

  /**
   * Initializes the source and destination Spanner databases, applies the generated DDL to the
   * source database, and randomly populates it with data.
   *
   * @param ddl The Schema configuration to apply to the source database.
   * @param numBatches The number of mutation batches to write to the source database. Set to 0 for
   *     an empty database test.
   */
  private void createAndPopulate(Ddl ddl, int numBatches) {
    // Initialize the databases with the appropriate dialect dynamically.
    sourceResourceManager =
        SpannerResourceManager.builder(
                testName + "-source",
                PROJECT,
                System.getProperty("spannerMultiRegion", "nam3"),
                ddl.dialect())
            .setNodeCount(2)
            .useCustomHost(spannerHost)
            .build();
    destResourceManager =
        SpannerResourceManager.builder(
                testName + "-dest",
                PROJECT,
                System.getProperty("spannerMultiRegion", "nam3"),
                ddl.dialect())
            .setNodeCount(2)
            .useCustomHost(spannerHost)
            .build();

    // Execute the schema statements on the source database.
    // The destination database is intentionally left entirely empty (no tables) so the
    // Import pipeline can completely reconstruct the schema on its own.
    sourceResourceManager.executeDdlStatements(ddl.statements());
    destResourceManager.executeDdlStatements(Collections.emptyList());

    if (numBatches > 0) {
      final Iterator<MutationGroup> mutations =
          new RandomInsertMutationGenerator(ddl).stream().iterator();

      for (int i = 0; i < numBatches; i++) {
        // We chunk the mutations into small batches of 10.
        // This is strictly required to prevent exceeding Spanner's 100MB / 20k mutation limits,
        // which would cause the integration test to crash on large random inserts.
        List<Mutation> batchMutations = new ArrayList<>();
        for (int j = 0; j < 10; j++) {
          MutationGroup m = mutations.next();
          m.forEach(batchMutations::add);
        }
        sourceResourceManager.write(batchMutations);
      }
    }
  }

  private void runTest() throws Exception {
    runTest(Dialect.GOOGLE_STANDARD_SQL);
  }

  /**
   * Executes the end-to-end flow: 1. Launches Export Pipeline to write source DB to GCS. 2.
   * Dynamically resolves the generated GCS path. 3. Launches Import Pipeline to read from GCS into
   * the destination DB. 4. Validates that the schema and data match exactly.
   *
   * @param dialect The Spanner dialect being tested.
   */
  private void runTest(Dialect dialect) throws Exception {
    String outputDir = getGcsPath("output_" + testName + "/");

    // ----------------------------------------------------------------------
    // 1. Run Export Pipeline
    // ----------------------------------------------------------------------
    LaunchConfig.Builder exportConfig =
        LaunchConfig.builder(testName, specPath)
            .addParameter("instanceId", sourceResourceManager.getInstanceId())
            .addParameter("databaseId", sourceResourceManager.getDatabaseId())
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("outputDir", outputDir);
    if (spannerHost != null) {
      exportConfig.addParameter("spannerHost", spannerHost);
    }

    LaunchInfo exportInfo = launchTemplate(exportConfig);
    Result exportResult = pipelineOperator().waitUntilDone(createConfig(exportInfo));
    assertThat(exportResult).isEqualTo(Result.LAUNCH_FINISHED);

    // ----------------------------------------------------------------------
    // 2. Run Import Pipeline
    // ----------------------------------------------------------------------
    Template importTemplate = ImportPipeline.class.getAnnotation(Template.class);
    String importSpecPath = getSpecPath(ImportPipeline.class, importTemplate, "pom.xml");

    // The Export pipeline generates a dynamic subdirectory name inside the outputDir.
    // Instead of guessing this path (which can cause FileNotFound failures if the internal Dataflow
    // Job IDs differ), we use the Artifact framework to fetch the exact Spanner JSON file.
    List<Artifact> artifacts =
        gcsClient.listArtifacts("output_" + testName, Pattern.compile(".*spanner-export\\.json$"));
    if (artifacts.isEmpty()) {
      throw new IllegalStateException("No spanner-export.json found under " + outputDir);
    }
    String spannerExportPath = artifacts.get(0).name();

    // We strip the filename itself off the artifact path to resolve the true target directory
    String importInputDir =
        "gs://"
            + gcsClient.getBucket()
            + "/"
            + spannerExportPath.substring(0, spannerExportPath.indexOf("spanner-export.json"));

    LaunchConfig.Builder importConfig =
        LaunchConfig.builder(testName, importSpecPath)
            .addParameter("instanceId", destResourceManager.getInstanceId())
            .addParameter("databaseId", destResourceManager.getDatabaseId())
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("inputDir", importInputDir)
            .addParameter("waitForIndexes", "true")
            .addParameter("waitForForeignKeys", "true")
            .addParameter("waitForChangeStreams", "true")
            .addParameter("waitForSequences", "true");
    if (spannerHost != null) {
      importConfig.addParameter("spannerHost", spannerHost);
    }

    LaunchInfo importInfo = launchTemplate(importConfig, importTemplate);
    Result importResult = pipelineOperator().waitUntilDone(createConfig(importInfo));
    assertThat(importResult).isEqualTo(Result.LAUNCH_FINISHED);

    // ----------------------------------------------------------------------
    // 3. Schema & Data Assertions
    // ----------------------------------------------------------------------
    // We are asserting that the schema of the source database matches the destination database
    // exactly. To do this, we read the parsed, tabular metadata from both databases using
    // InformationSchemaScanner and reconstruct them into Ddl object models. We then print these
    // models canonically and compare their strings.
    //
    // IMPORTANT: We cannot use SpannerResourceManager.getDatabaseDdl() directly for this assertion.
    // getDatabaseDdl() returns the raw strings used to create the tables. For randomly generated
    // schemas, these strings contain un-normalized formatting (e.g., randomized WHERE column
    // order).
    // The Dataflow Export pipeline normalizes the schema (alphabetizes columns, etc.) when writing
    // it to spanner-export.json, which the Import pipeline then executes. As a result, the source
    // and destination would have functionally identical but textually mismatched DDL strings.
    //
    // The InformationSchemaScanner guarantees a true structural comparison without arbitrary
    // text-formatting false positives.
    Ddl destinationDdl = readDdl(destResourceManager, dialect);
    Ddl sourceDdl = readDdl(sourceResourceManager, dialect);

    // Ensure the entire structural representation of the schemas (types, lengths, keys) is
    // identical.
    assertThat(destinationDdl.prettyPrint()).isEqualTo(sourceDdl.prettyPrint());

    // Iterate through every single table and systematically ensure every row and column is
    // identical.
    for (Table table : destinationDdl.allTables()) {
      List<String> columnNames =
          table.columns().stream()
              .filter(
                  c ->
                      !(c.isGenerated() && !c.isStored())
                          && !c.typeString().toUpperCase().contains("TOKENLIST"))
              .map(Column::name)
              .collect(Collectors.toList());

      List<Struct> sourceRecords =
          sourceResourceManager.readTableRecords(table.name(), columnNames);
      List<Struct> destRecords = destResourceManager.readTableRecords(table.name(), columnNames);

      // assertThat(...).containsExactlyElementsIn ignores absolute ordering,
      // which is required since distributed Spanner queries do not guarantee return order.
      assertThat(destRecords).containsExactlyElementsIn(sourceRecords);
    }
  }

  private Ddl readDdl(SpannerResourceManager resourceManager, Dialect dialect) {
    DatabaseClient dbClient = resourceManager.getDatabaseClient();
    Ddl ddl;
    try (ReadOnlyTransaction ctx = dbClient.readOnlyTransaction()) {
      ddl = new InformationSchemaScanner(ctx, dialect).scan();
    }
    return ddl;
  }

  private void createAndPopulate(String sqlFile, Dialect dialect, int numBatches) throws Exception {
    sourceResourceManager =
        SpannerResourceManager.builder(
                testName + "-source",
                PROJECT,
                System.getProperty("spannerMultiRegion", "nam3"),
                dialect)
            .setNodeCount(2)
            .useCustomHost(spannerHost)
            .build();
    destResourceManager =
        SpannerResourceManager.builder(
                testName + "-dest",
                PROJECT,
                System.getProperty("spannerMultiRegion", "nam3"),
                dialect)
            .setNodeCount(2)
            .useCustomHost(spannerHost)
            .build();

    // Read the SQL statements from the static file
    String ddlString =
        String.join(
            " ",
            Resources.readLines(Resources.getResource(sqlFile), StandardCharsets.UTF_8).stream()
                .map(line -> line.replaceAll("\\s*--.*$", ""))
                .collect(ImmutableList.toImmutableList()));
    ddlString =
        ddlString
            .trim()
            .replaceAll("%PROJECT_ID%", PROJECT)
            .replaceAll("%DATABASE_NAME%", sourceResourceManager.getDatabaseId());
    List<String> ddlStatements =
        Arrays.stream(ddlString.split(";")).filter(d -> !d.isBlank()).collect(Collectors.toList());

    // Execute the schema statements on the source database.
    sourceResourceManager.executeDdlStatements(ddlStatements);
    destResourceManager.executeDdlStatements(Collections.emptyList());

    if (numBatches > 0) {
      // Use InformationSchemaScanner to dynamically extract the loaded schema into our Ddl object
      Ddl ddl = readDdl(sourceResourceManager, dialect);
      final Iterator<MutationGroup> mutations =
          new RandomInsertMutationGenerator(ddl).stream().iterator();

      for (int i = 0; i < numBatches; i++) {
        // We chunk the mutations into small batches of 10.
        // This is strictly required to prevent exceeding Spanner's 100MB / 20k mutation limits,
        // which would cause the integration test to crash on large random inserts.
        List<Mutation> batchMutations = new ArrayList<>();
        for (int j = 0; j < 10; j++) {
          MutationGroup m = mutations.next();
          m.forEach(batchMutations::add);
        }
        sourceResourceManager.write(batchMutations);
      }
    }
  }

  @Test
  public void testAllSchemaAndDataGsql() throws Exception {
    createAndPopulate("CopyDbIT-AllSchemaAndData-gsql.sql", Dialect.GOOGLE_STANDARD_SQL, 100);
    runTest(Dialect.GOOGLE_STANDARD_SQL);
  }

  @Test
  public void testAllSchemaAndDataPg() throws Exception {
    createAndPopulate("CopyDbIT-AllSchemaAndData-pg.sql", Dialect.POSTGRESQL, 100);
    runTest(Dialect.POSTGRESQL);
  }

  @Test
  public void testEmptyDbGsql() throws Exception {
    Ddl ddl = Ddl.builder(Dialect.GOOGLE_STANDARD_SQL).build();
    createAndPopulate(ddl, 0);
    runTest(Dialect.GOOGLE_STANDARD_SQL);
  }
}
