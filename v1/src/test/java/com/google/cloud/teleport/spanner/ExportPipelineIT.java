/*
 * Copyright (C) 2023 Google LLC
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
import static org.apache.beam.it.gcp.artifacts.matchers.ArtifactAsserts.assertThatGenericRecords;
import static org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts.mutationsToRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.artifacts.utils.AvroTestUtil;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link ExportPipeline Spanner to GCS Avro} template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(ExportPipeline.class)
@RunWith(JUnit4.class)
public class ExportPipelineIT extends TemplateTestBase {

  private static final int MESSAGES_COUNT = 100;

  private static final Schema EMPTY_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\n"
                  + "  \"type\": \"record\",\n"
                  + "  \"name\": \"EmptyTable\",\n"
                  + "  \"namespace\": \"com.google.cloud.teleport.spanner\",\n"
                  + "  \"fields\": [\n"
                  + "    {\n"
                  + "      \"name\": \"id\", \"type\": \"long\", \"sqlType\": \"INT64\" }\n"
                  + "  ]\n"
                  + "}");

  private static final Schema SINGERS_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\n"
                  + "  \"type\": \"record\",\n"
                  + "  \"name\": \"Singers\",\n"
                  + "  \"namespace\": \"com.google.cloud.teleport.spanner\",\n"
                  + "  \"fields\": [\n"
                  + "    { \"name\": \"Id\", \"type\": \"long\", \"sqlType\": \"INT64\" },\n"
                  + "    { \"name\": \"FirstName\", \"type\": \"string\" },\n"
                  + "    { \"name\": \"LastName\", \"type\": \"string\" }\n"
                  + "  ]\n"
                  + "}");

  private static final Schema MODEL_STRUCT_SCHEMA =
      new Schema.Parser()
          .parse(
              "{"
                  + "  \"type\": \"record\","
                  + "  \"name\": \"ModelStruct\","
                  + "  \"namespace\": \"com.google.cloud.teleport.spanner\","
                  + "  \"fields\": ["
                  + "    { \"name\": \"Input\", "
                  + "      \"type\": { "
                  + "        \"type\": \"record\", "
                  + "        \"name\":\"ModelStruct_Input\", "
                  + "        \"fields\":["
                  + "          {"
                  + "            \"name\":\"content\","
                  + "            \"type\":\"string\","
                  + "            \"sqlType\":\"STRING(MAX)\","
                  + "            \"spannerOption_0\":\"required=TRUE\""
                  + "          }"
                  + "        ]"
                  + "      }"
                  + "    },"
                  + "    { \"name\": \"Output\", "
                  + "      \"type\": {"
                  + "        \"type\": \"record\", "
                  + "        \"name\":\"ModelStruct_Output\", "
                  + "        \"fields\":["
                  + "          {"
                  + "            \"name\":\"embeddings\","
                  + "            \"type\":{"
                  + "              \"type\":\"record\","
                  + "              \"name\":\"ModelStruct_struct_output_0\","
                  + "              \"namespace\":\"\","
                  + "              \"fields\":["
                  + "                {"
                  + "                  \"name\":\"statistics\","
                  + "                  \"type\":{"
                  + "                    \"type\":\"record\","
                  + "                    \"name\":\"ModelStruct_struct_output_0_1\","
                  + "                    \"fields\":["
                  + "                      {\"name\":\"truncated\",\"type\":\"boolean\"},"
                  + "                      {\"name\":\"token_count\",\"type\":\"double\"}"
                  + "                    ]"
                  + "                  }"
                  + "                },"
                  + "                {"
                  + "                  \"name\":\"values\","
                  + "                  \"type\":{\"type\":\"array\",\"items\":[\"null\",\"double\"]}"
                  + "                }"
                  + "              ]"
                  + "            },"
                  + "            \"sqlType\":\"STRUCT<statistics STRUCT<truncated BOOL, token_count FLOAT64>, values ARRAY<FLOAT64>>\","
                  + "            \"spannerOption_0\":\"required=TRUE\""
                  + "          }"
                  + "        ]"
                  + "      }"
                  + "    }"
                  + "  ]"
                  + "}");
  private SpannerResourceManager googleSqlResourceManager;
  private SpannerResourceManager postgresResourceManager;

  @Before
  public void setup() throws IOException {
    // Set up resource managers
    googleSqlResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION).maybeUseStaticInstance().build();
    postgresResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.POSTGRESQL)
            .maybeUseStaticInstance()
            .build();
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(googleSqlResourceManager, postgresResourceManager);
  }

  @Test
  public void testSpannerToGCSAvro() throws IOException {
    // Arrange
    String createEmptyTableStatement =
        String.format(
            "CREATE TABLE `%s_EmptyTable` (\n" + "  id INT64 NOT NULL,\n" + ") PRIMARY KEY(id)",
            testName);
    String createSingersTableStatement =
        String.format(
            "CREATE TABLE `%s_Singers` (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  FirstName String(1024),\n"
                + "  LastName String(1024),\n"
                + ") PRIMARY KEY(Id)",
            testName);
    String createModelStructStatement =
        String.format(
            "CREATE MODEL `%s_ModelStruct`\n"
                + " INPUT(content STRING(MAX)) \n"
                + " OUTPUT (embeddings STRUCT<statistics STRUCT<truncated BOOL, token_count FLOAT64>, values ARRAY<FLOAT64>>) \n"
                + " REMOTE OPTIONS (endpoint=\"//aiplatform.googleapis.com/projects/span-cloud-testing/locations/us-central1/publishers/google/models/textembedding-gecko\")",
            testName);

    googleSqlResourceManager.executeDdlStatement(createEmptyTableStatement);
    googleSqlResourceManager.executeDdlStatement(createSingersTableStatement);
    googleSqlResourceManager.executeDdlStatement(createModelStructStatement);
    List<Mutation> expectedData = generateTableRows(String.format("%s_Singers", testName));
    googleSqlResourceManager.write(expectedData);
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("instanceId", googleSqlResourceManager.getInstanceId())
            .addParameter("databaseId", googleSqlResourceManager.getDatabaseId())
            //.addParameter("outputDir", getGcsPath("output/"))
            .addParameter("outputDir", "gs://djagaluru-teleport-integration-test/exported-data/gsql/")
            .addParameter("spannerHost", "https://staging-wrenchworks.sandbox.googleapis.com");

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> singersArtifacts =
        gcsClient.listArtifacts(
            "output/", Pattern.compile(String.format(".*/%s_%s.*\\.avro.*", testName, "Singers")));
    List<Artifact> emptyArtifacts =
        gcsClient.listArtifacts(
            "output/", Pattern.compile(String.format(".*/%s_%s.*\\.avro.*", testName, "Empty")));
    List<Artifact> modelStructArtifacts =
        gcsClient.listArtifacts(
            "output/",
            Pattern.compile(String.format(".*/%s_%s.*\\.avro.*", testName, "ModelStruct")));
    assertThat(singersArtifacts).isNotEmpty();
    assertThat(emptyArtifacts).isNotEmpty();
    assertThat(modelStructArtifacts).isNotEmpty();

    List<GenericRecord> singersRecords = extractArtifacts(singersArtifacts, SINGERS_SCHEMA);
    List<GenericRecord> emptyRecords = extractArtifacts(emptyArtifacts, EMPTY_SCHEMA);
    List<GenericRecord> modelStructRecords =
        extractArtifacts(modelStructArtifacts, MODEL_STRUCT_SCHEMA);

    assertThatGenericRecords(singersRecords)
        .hasRecordsUnorderedCaseInsensitiveColumns(mutationsToRecords(expectedData));
    assertThatGenericRecords(emptyRecords).hasRows(0);
    assertThatGenericRecords(modelStructRecords).hasRows(0);
  }

  @Test
  public void testPostgresSpannerToGCSAvro() throws IOException {
    // Arrange
    String createEmptyTableStatement =
        String.format(
            "CREATE TABLE \"%s_EmptyTable\" (\n" + "  id bigint NOT NULL,\nPRIMARY KEY(id)\n" + ")",
            testName);
    String createSingersTableStatement =
        String.format(
            "CREATE TABLE \"%s_Singers\" (\n"
                + "  \"Id\" bigint,\n"
                + "  \"FirstName\" character varying(256),\n"
                + "  \"LastName\" character varying(256),\n"
                + "PRIMARY KEY(\"Id\"))",
            testName);

    postgresResourceManager.executeDdlStatement(createEmptyTableStatement);
    postgresResourceManager.executeDdlStatement(createSingersTableStatement);
    List<Mutation> expectedData = generateTableRows(String.format("%s_Singers", testName));
    postgresResourceManager.write(expectedData);
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("instanceId", postgresResourceManager.getInstanceId())
            .addParameter("databaseId", postgresResourceManager.getDatabaseId())
            //.addParameter("outputDir", getGcsPath("output/"))
            .addParameter("outputDir", "gs://djagaluru-teleport-integration-test/exported-data/postgres/")
            .addParameter("spannerHost", "https://staging-wrenchworks.sandbox.googleapis.com");

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> singersArtifacts =
        gcsClient.listArtifacts(
            "output/", Pattern.compile(String.format(".*/%s_%s.*\\.avro.*", testName, "Singers")));
    List<Artifact> emptyArtifacts =
        gcsClient.listArtifacts(
            "output/", Pattern.compile(String.format(".*/%s_%s.*\\.avro.*", testName, "Empty")));
    assertThat(singersArtifacts).isNotEmpty();
    assertThat(emptyArtifacts).isNotEmpty();

    List<GenericRecord> singersRecords = extractArtifacts(singersArtifacts, SINGERS_SCHEMA);
    List<GenericRecord> emptyRecords = extractArtifacts(emptyArtifacts, EMPTY_SCHEMA);

    assertThatGenericRecords(singersRecords)
        .hasRecordsUnorderedCaseInsensitiveColumns(mutationsToRecords(expectedData));
    assertThatGenericRecords(emptyRecords).hasRows(0);
  }

  private static List<Mutation> generateTableRows(String tableId) {
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < MESSAGES_COUNT; i++) {
      Mutation.WriteBuilder mutation = Mutation.newInsertBuilder(tableId);
      mutation.set("Id").to(i);
      mutation.set("FirstName").to(RandomStringUtils.randomAlphanumeric(1, 20));
      mutation.set("LastName").to(RandomStringUtils.randomAlphanumeric(1, 20));
      mutations.add(mutation.build());
    }

    return mutations;
  }

  private static List<GenericRecord> extractArtifacts(List<Artifact> artifacts, Schema schema) {
    List<GenericRecord> records = new ArrayList<>();
    artifacts.forEach(
        artifact -> {
          try {
            records.addAll(AvroTestUtil.readRecords(schema, artifact.contents()));
          } catch (IOException e) {
            throw new RuntimeException("Error reading " + artifact.name() + " as Avro.", e);
          }
        });

    return records;
  }
}
