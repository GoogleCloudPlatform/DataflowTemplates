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
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.spanner.ddl.Column;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.spanner.ddl.RandomDdlGenerator;
import com.google.cloud.teleport.spanner.ddl.RandomInsertMutationGenerator;
import com.google.cloud.teleport.spanner.ddl.Table;
import com.google.cloud.teleport.spanner.spannerio.MutationGroup;
import java.util.ArrayList;
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration test for {@link ExportPipeline} and {@link ImportPipeline}.
 *
 * <p>This test completely validates the entire lifecycle of exporting a Spanner database to GCS
 * using Avro format, and subsequently importing that exact data into a fresh Spanner database. It
 * focuses on testing with randomly generated schemas and data.
 */
@Category(IntegrationTest.class)
@TemplateIntegrationTest(ExportPipeline.class)
@RunWith(JUnit4.class)
public class CopyDbRandomisedIT extends SpannerTemplateITBase {

  private SpannerResourceManager sourceResourceManager;
  private SpannerResourceManager destResourceManager;

  @Before
  public void setup() {}

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(sourceResourceManager, destResourceManager);
  }

  private void createAndPopulate(Ddl ddl, int numBatches) {
    sourceResourceManager =
        SpannerResourceManager.builder(testName + "-source", PROJECT, "nam3", ddl.dialect())
            .useCustomHost(spannerHost)
            .build();
    destResourceManager =
        SpannerResourceManager.builder(testName + "-dest", PROJECT, "nam3", ddl.dialect())
            .useCustomHost(spannerHost)
            .build();

    sourceResourceManager.executeDdlStatements(ddl.statements());
    destResourceManager.executeDdlStatements(Collections.emptyList());

    if (numBatches > 0) {
      final Iterator<MutationGroup> mutations =
          new RandomInsertMutationGenerator(ddl).stream().iterator();

      for (int i = 0; i < numBatches; i++) {
        List<Mutation> batchMutations = new ArrayList<>();
        for (int j = 0; j < 10; j++) {
          MutationGroup m = mutations.next();
          m.forEach(batchMutations::add);
        }
        sourceResourceManager.write(batchMutations);
      }
    }
  }

  private void runTest(Dialect dialect) throws Exception {
    String outputDir = getGcsPath("output_" + testName + "/");

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

    Template importTemplate = ImportPipeline.class.getAnnotation(Template.class);
    String importSpecPath = getSpecPath(ImportPipeline.class, importTemplate, "pom.xml");

    List<Artifact> artifacts =
        gcsClient.listArtifacts("output_" + testName, Pattern.compile(".*spanner-export\\.json$"));
    if (artifacts.isEmpty()) {
      throw new IllegalStateException("No spanner-export.json found under " + outputDir);
    }
    String spannerExportPath = artifacts.get(0).name();

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

    Ddl destinationDdl = readDdl(destResourceManager, dialect);
    Ddl sourceDdl = readDdl(sourceResourceManager, dialect);

    assertThat(destinationDdl.prettyPrint()).isEqualTo(sourceDdl.prettyPrint());

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

  @Test
  public void testRandomSchemaAndDataGsql() throws Exception {
    Ddl ddl = RandomDdlGenerator.builder().build().generate();
    createAndPopulate(ddl, 100);
    runTest(Dialect.GOOGLE_STANDARD_SQL);
  }

  @Test
  public void testRandomSchemaAndDataPg() throws Exception {
    Ddl ddl = RandomDdlGenerator.builder(Dialect.POSTGRESQL).build().generate();
    createAndPopulate(ddl, 100);
    runTest(Dialect.POSTGRESQL);
  }
}
