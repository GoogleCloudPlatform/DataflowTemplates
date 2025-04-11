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
import com.google.cloud.teleport.metadata.SpannerStagingTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.artifacts.utils.AvroTestUtil;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerTemplateITBase;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Integration test for {@link ExportPipeline Spanner to GCS Avro} template. */
@Category({TemplateIntegrationTest.class, SpannerStagingTest.class})
@TemplateIntegrationTest(ExportPipeline.class)
@RunWith(Parameterized.class)
public class ExportPipelineIT extends SpannerTemplateITBase {

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
                  + "    { \"name\": \"LastName\", \"type\": \"string\" },\n"
                  + "    { \"name\": \"Rating\", \"type\": \"float\" }\n"
                  + "  ]\n"
                  + "}");

  private static final Schema UUID_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\n"
                  + "  \"type\": \"record\",\n"
                  + "  \"name\": \"UuidTable\",\n"
                  + "  \"namespace\": \"com.google.cloud.teleport.spanner\",\n"
                  + "  \"fields\": [\n"
                  + "    { \"name\": \"Id\", \"type\": \"long\", \"sqlType\": \"INT64\" },\n"
                  + "    { \"name\": \"UuidCol\", \"type\": [\"null\",\"string\"], \"sqlType\": \"UUID\" },\n"
                  + "    { \"name\": \"UuidArrayCol\", \"type\": [\"null\", { "
                  + "          \"type\": \"array\", "
                  + "          \"items\": \"string\" "
                  + "    }], \"sqlType\": \"ARRAY<UUID>\" }\n"
                  + "  ]\n"
                  + "}");

  private static final Schema IDENTITY_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\n"
                  + "  \"type\": \"record\",\n"
                  + "  \"name\": \"Identity\",\n"
                  + "  \"namespace\": \"com.google.cloud.teleport.spanner\",\n"
                  + "  \"fields\": [\n"
                  + "    { \"name\": \"Id\", \"type\": \"long\", \"sqlType\": \"INT64\", \"sequenceKind\":\"bit_reversed_positive\", \"identityColumn\":\"true\" },\n"
                  + "    { \"name\": \"NonKeyIdCol1\", \"type\": \"long\", \"sqlType\": \"INT64\", \"identityColumn\":\"true\" },\n"
                  + "    { \"name\": \"NonKeyIdCol2\", \"type\": \"long\", \"sqlType\": \"INT64\", \"skipRangeMin\":\"1000\",\"skipRangeMax\":\"2000\", \"identityColumn\":\"true\" }\n"
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

  private SpannerResourceManager spannerResourceManager;

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void testSpannerToAvro() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.GOOGLE_STANDARD_SQL)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .build();
    testSpannerToGCSAvroBase(
        paramAdder ->
            paramAdder.addParameter("spannerHost", spannerResourceManager.getSpannerHost()));
  }

  private void testSpannerToGCSAvroBase(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {
    String setDefaultTimeZoneStatement =
        "ALTER DATABASE db SET OPTIONS (default_time_zone = 'UTC')";
    // Setting default time zone needs to be the first statement because it requires
    // an empty database without any tables.
    spannerResourceManager.executeDdlStatement(setDefaultTimeZoneStatement);

    String resourceFileName = "ExportPipelineIT/spanner-gsql-ddl.sql";
    String ddl =
        String.join(
                " ",
                Resources.readLines(
                    Resources.getResource(resourceFileName), StandardCharsets.UTF_8))
            .replaceAll("%PREFIX%", testName);
    ddl = ddl.trim();
    List<String> ddls = Arrays.stream(ddl.split(";")).filter(d -> !d.isBlank()).toList();
    spannerResourceManager.executeDdlStatements(ddls);

    List<Mutation> expectedData = generateTableRows(String.format("%s_Singers", testName));
    spannerResourceManager.write(expectedData);
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("spannerProjectId", PROJECT)
                .addParameter("instanceId", spannerResourceManager.getInstanceId())
                .addParameter("databaseId", spannerResourceManager.getDatabaseId())
                .addParameter("outputDir", getGcsPath("output/")));

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
    List<Artifact> searchIndexArtifacts =
        gcsClient.listArtifacts(
            "output/",
            Pattern.compile(String.format(".*/%s_%s.*\\.avro.*", testName, "SearchIndex")));
    List<Artifact> identityArtifacts =
        gcsClient.listArtifacts(
            "output/", Pattern.compile(String.format(".*/%s_%s.*\\.avro.*", testName, "Identity")));
    List<Artifact> sequenceArtifacts =
        gcsClient.listArtifacts(
            "output/",
            Pattern.compile(String.format(".*/%s_%s.*\\.avro.*", testName, "Sequence1")));
    List<Artifact> sequenceNoKindArtifacts =
        gcsClient.listArtifacts(
            "output/",
            Pattern.compile(String.format(".*/%s_%s.*\\.avro.*", testName, "Sequence2")));
    assertThat(singersArtifacts).isNotEmpty();
    assertThat(emptyArtifacts).isNotEmpty();
    assertThat(modelStructArtifacts).isNotEmpty();
    assertThat(identityArtifacts).isNotEmpty();
    assertThat(sequenceArtifacts).isNotEmpty();
    assertThat(sequenceNoKindArtifacts).isNotEmpty();

    List<GenericRecord> singersRecords = extractArtifacts(singersArtifacts, SINGERS_SCHEMA);
    List<GenericRecord> emptyRecords = extractArtifacts(emptyArtifacts, EMPTY_SCHEMA);
    List<GenericRecord> modelStructRecords =
        extractArtifacts(modelStructArtifacts, MODEL_STRUCT_SCHEMA);
    List<GenericRecord> identityRecords = extractArtifacts(identityArtifacts, IDENTITY_SCHEMA);

    assertThatGenericRecords(singersRecords)
        .hasRecordsUnorderedCaseInsensitiveColumns(mutationsToRecords(expectedData));
    assertThatGenericRecords(emptyRecords).hasRows(0);
    assertThatGenericRecords(modelStructRecords).hasRows(0);
    assertThatGenericRecords(identityRecords).hasRows(0);
  }

  @Test
  public void testPGSpannerToAvro() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.POSTGRESQL)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .build();
    testPGSpannerToAvroBase(
        paramAdder ->
            paramAdder.addParameter("spannerHost", spannerResourceManager.getSpannerHost()));
  }

  private void testPGSpannerToAvroBase(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {
    String tableNamePrefix = testName.substring(0, 15);

    String setDefaultTimeZoneStatement = "ALTER DATABASE db SET spanner.default_time_zone = 'UTC'";
    // Setting default time zone needs to be the first statement because it requires
    // an empty database without any tables.
    spannerResourceManager.executeDdlStatement(setDefaultTimeZoneStatement);

    String resourceFileName = "ExportPipelineIT/spanner-pg-ddl.sql";
    String ddl =
        String.join(
                " ",
                Resources.readLines(
                    Resources.getResource(resourceFileName), StandardCharsets.UTF_8))
            .replaceAll("%PREFIX%", tableNamePrefix);
    ddl = ddl.trim();
    List<String> ddls = Arrays.stream(ddl.split(";")).filter(d -> !d.isBlank()).toList();
    spannerResourceManager.executeDdlStatements(ddls);

    List<Mutation> expectedData = generateTableRows(String.format("%s_Singers", tableNamePrefix));
    spannerResourceManager.write(expectedData);
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("spannerProjectId", PROJECT)
                .addParameter("instanceId", spannerResourceManager.getInstanceId())
                .addParameter("databaseId", spannerResourceManager.getDatabaseId())
                .addParameter("outputDir", getGcsPath("output/")));

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> singersArtifacts =
        gcsClient.listArtifacts(
            "output/",
            Pattern.compile(String.format(".*/%s_%s.*\\.avro.*", tableNamePrefix, "Singers")));
    List<Artifact> emptyArtifacts =
        gcsClient.listArtifacts(
            "output/",
            Pattern.compile(String.format(".*/%s_%s.*\\.avro.*", tableNamePrefix, "Empty")));
    List<Artifact> searchIndexArtifacts =
        gcsClient.listArtifacts(
            "output/",
            Pattern.compile(String.format(".*/%s_%s.*\\.avro.*", tableNamePrefix, "SearchIndex")));
    List<Artifact> identityArtifacts =
        gcsClient.listArtifacts(
            "output/",
            Pattern.compile(String.format(".*/%s_%s.*\\.avro.*", tableNamePrefix, "Identity")));
    List<Artifact> sequenceArtifacts =
        gcsClient.listArtifacts(
            "output/",
            Pattern.compile(String.format(".*/%s_%s.*\\.avro.*", tableNamePrefix, "Sequence1")));
    List<Artifact> sequenceNoKindArtifacts =
        gcsClient.listArtifacts(
            "output/",
            Pattern.compile(String.format(".*/%s_%s.*\\.avro.*", tableNamePrefix, "Sequence2")));
    assertThat(singersArtifacts).isNotEmpty();
    assertThat(emptyArtifacts).isNotEmpty();
    assertThat(identityArtifacts).isNotEmpty();
    assertThat(sequenceArtifacts).isNotEmpty();
    assertThat(sequenceNoKindArtifacts).isNotEmpty();

    List<GenericRecord> singersRecords = extractArtifacts(singersArtifacts, SINGERS_SCHEMA);
    List<GenericRecord> emptyRecords = extractArtifacts(emptyArtifacts, EMPTY_SCHEMA);

    assertThatGenericRecords(singersRecords)
        .hasRecordsUnorderedCaseInsensitiveColumns(mutationsToRecords(expectedData));
    assertThatGenericRecords(emptyRecords).hasRows(0);

    List<GenericRecord> identityRecords = extractArtifacts(identityArtifacts, IDENTITY_SCHEMA);
    assertThatGenericRecords(identityRecords).hasRows(0);
  }

  // TODO(b/395532087): Consolidate this with other tests after UUID launch.
  @Test
  public void testSpannerToAvro_UUID() throws IOException {
    // Run only on staging environment
    if (!SpannerResourceManager.STAGING_SPANNER_HOST.equals(spannerHost)) {
      return;
    }
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.GOOGLE_STANDARD_SQL)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .build();

    String resourceFileName = "ExportPipelineIT/spanner-gsql-uuid-ddl.sql";
    String ddl =
        String.join(
                " ",
                Resources.readLines(
                    Resources.getResource(resourceFileName), StandardCharsets.UTF_8))
            .replaceAll("%PREFIX%", testName);
    ddl = ddl.trim();
    List<String> ddls = Arrays.stream(ddl.split(";")).filter(d -> !d.isBlank()).toList();
    spannerResourceManager.executeDdlStatements(ddls);

    List<Mutation> expectedData = generateTableRowsUUID(String.format("%s_UuidTable", testName));
    spannerResourceManager.write(expectedData);
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("outputDir", getGcsPath("output/"))
            .addParameter("spannerHost", spannerResourceManager.getSpannerHost());

    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    assertThatResult(result).isLaunchFinished();

    List<Artifact> uuidTableArtifacts =
        gcsClient.listArtifacts(
            "output/", Pattern.compile(String.format(".*%s.*\\.avro.*", "UuidTable")));
    assertThat(uuidTableArtifacts).isNotEmpty();

    List<GenericRecord> uuidTableRecords = extractArtifacts(uuidTableArtifacts, UUID_SCHEMA);

    assertThatGenericRecords(uuidTableRecords)
        .hasRecordsUnordered(mutationsToRecords(expectedData));
  }

  // TODO(b/395532087): Consolidate this with other tests after UUID launch.
  @Test
  public void testPGSpannerToAvro_UUID() throws IOException {
    // Run only on staging environment
    if (!SpannerResourceManager.STAGING_SPANNER_HOST.equals(spannerHost)) {
      return;
    }
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.POSTGRESQL)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .build();

    String resourceFileName = "ExportPipelineIT/spanner-pg-uuid-ddl.sql";
    String ddl =
        String.join(
                " ",
                Resources.readLines(
                    Resources.getResource(resourceFileName), StandardCharsets.UTF_8))
            .replaceAll("%PREFIX%", testName);
    ddl = ddl.trim();
    List<String> ddls = Arrays.stream(ddl.split(";")).filter(d -> !d.isBlank()).toList();
    spannerResourceManager.executeDdlStatements(ddls);

    List<Mutation> expectedData = generateTableRowsUUID(String.format("%s_UuidTable", testName));
    spannerResourceManager.write(expectedData);
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("outputDir", getGcsPath("output/"))
            .addParameter("spannerHost", spannerResourceManager.getSpannerHost());

    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    assertThatResult(result).isLaunchFinished();

    List<Artifact> uuidTableArtifacts =
        gcsClient.listArtifacts(
            "output/", Pattern.compile(String.format(".*%s.*\\.avro.*", "UuidTable")));
    assertThat(uuidTableArtifacts).isNotEmpty();

    List<GenericRecord> uuidTableRecords = extractArtifacts(uuidTableArtifacts, UUID_SCHEMA);

    assertThatGenericRecords(uuidTableRecords)
        .hasRecordsUnordered(mutationsToRecords(expectedData));
  }

  private static List<Mutation> generateTableRows(String tableId) {
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < MESSAGES_COUNT; i++) {
      Mutation.WriteBuilder mutation = Mutation.newInsertBuilder(tableId);
      mutation.set("Id").to(i);
      mutation.set("FirstName").to(RandomStringUtils.randomAlphanumeric(1, 20));
      mutation.set("LastName").to(RandomStringUtils.randomAlphanumeric(1, 20));
      mutation.set("Rating").to(RandomUtils.nextFloat());
      mutations.add(mutation.build());
    }

    return mutations;
  }

  private static List<Mutation> generateTableRowsUUID(String tableId) {
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < MESSAGES_COUNT; i++) {
      Mutation.WriteBuilder mutation = Mutation.newInsertBuilder(tableId);
      mutation.set("Id").to(i);
      String uuid = Math.random() < 0.5 ? UUID.randomUUID().toString() : null;
      List<String> uuidArray = Math.random() < 0.5 ? generateUuidArray() : null;
      mutation.set("UuidCol").to(uuid);
      mutation.set("UuidArrayCol").toStringArray(uuidArray);
      mutations.add(mutation.build());
    }
    return mutations;
  }

  private static List<String> generateUuidArray() {
    int size = (int) (Math.random() * (10));
    List<String> uuids = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      uuids.add(UUID.randomUUID().toString());
    }
    return uuids;
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
