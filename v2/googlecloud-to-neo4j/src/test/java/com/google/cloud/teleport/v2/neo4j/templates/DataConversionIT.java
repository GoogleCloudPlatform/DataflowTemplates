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
package com.google.cloud.teleport.v2.neo4j.templates;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.neo4j.DatabaseWaitOptions;
import org.apache.beam.it.neo4j.Neo4jResourceManager;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(GoogleCloudToNeo4j.class)
@RunWith(JUnit4.class)
public class DataConversionIT extends TemplateTestBase {

  private BigQueryResourceManager bigQueryClient;
  private Neo4jResourceManager neo4jClient;

  @Before
  public void setUp() {
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    neo4jClient =
        Neo4jResourceManager.builder(testName)
            .setDatabaseName(null, DatabaseWaitOptions.waitDatabase(60))
            .setAdminPassword("letmein!")
            .setHost(TestProperties.hostIp())
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryClient, neo4jClient);
  }

  // NOTE: BIGNUMERIC, GEOGRAPHY, JSON and INTERVAL BigQuery column types are not supported by Beam
  @SuppressWarnings("unchecked")
  @Test
  public void supportsBigQueryDataTypes() throws Exception {
    TableId table =
        bigQueryClient.createTable(
            testName,
            Schema.of(
                Field.newBuilder("bool", StandardSQLTypeName.BOOL).build(),
                Field.newBuilder("int64", StandardSQLTypeName.INT64).build(),
                Field.newBuilder("float64", StandardSQLTypeName.FLOAT64).build(),
                Field.newBuilder("numeric", StandardSQLTypeName.NUMERIC).build(),
                Field.newBuilder("string", StandardSQLTypeName.STRING).build(),
                Field.newBuilder("bytes", StandardSQLTypeName.BYTES).build(),
                Field.newBuilder("date", StandardSQLTypeName.DATE).build(),
                Field.newBuilder("time", StandardSQLTypeName.TIME).build(),
                Field.newBuilder("datetime", StandardSQLTypeName.DATETIME).build(),
                Field.newBuilder("timestamp", StandardSQLTypeName.TIMESTAMP).build(),
                // This is currently mapped in the spec file as a STRING just for ensuring the field
                // is mapped. Property mappings for BQ sources do not enforce type conversion.
                Field.newBuilder("array_of_int64", StandardSQLTypeName.INT64)
                    .setMode(Field.Mode.REPEATED)
                    .build()));
    Map<String, Object> sourceRow = new HashMap<>();
    sourceRow.put("bool", true);
    sourceRow.put("int64", 10L);
    sourceRow.put("float64", 40D);
    sourceRow.put("numeric", 50D);
    sourceRow.put("string", "string");
    sourceRow.put(
        "bytes",
        Base64.getEncoder().encodeToString(new byte[] {(byte) 0x89, (byte) 0x65, (byte) 0x89}));
    sourceRow.put("date", "1999-12-01");
    sourceRow.put("time", "12:13:14.123456");
    sourceRow.put("datetime", "2019-02-17 11:24:02.234567");
    sourceRow.put("timestamp", "2019-02-17 11:24:01.123456Z");
    sourceRow.put("array_of_int64", new long[] {1, 2, 3, 4, 5, 6, 7});
    bigQueryClient.write(testName, List.of(RowToInsert.of(sourceRow)));
    gcsClient.createArtifact("spec.json", contentOf("/testing-specs/data-conversion/bq-spec.json"));
    gcsClient.createArtifact(
        "neo4j.json",
        String.format(
            "{\n"
                + "  \"server_url\": \"%s\",\n"
                + "  \"database\": \"%s\",\n"
                + "  \"auth_type\": \"basic\",\n"
                + "  \"username\": \"neo4j\",\n"
                + "  \"pwd\": \"%s\"\n"
                + "}",
            neo4jClient.getUri(), neo4jClient.getDatabaseName(), neo4jClient.getAdminPassword()));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jobSpecUri", getGcsPath("spec.json"))
            .addParameter("neo4jConnectionUri", getGcsPath("neo4j.json"))
            .addParameter(
                "optionsJson", String.format("{\"bqtable\": \"%s\"}", toTableSpecStandard(table)));
    LaunchInfo info = launchTemplate(options);

    Map<String, Object> expectedRow = new HashMap<>();
    expectedRow.put("bool", true);
    expectedRow.put("int64", 10L);
    expectedRow.put("float64", 40D);
    expectedRow.put("numeric", 50D);
    expectedRow.put("string", "string");
    expectedRow.put("bytes", new byte[] {(byte) 0x89, (byte) 0x65, (byte) 0x89});
    expectedRow.put("date", LocalDate.of(1999, 12, 1));
    expectedRow.put("time", LocalTime.of(12, 13, 14, 123456000));
    expectedRow.put("datetime", LocalDateTime.of(2019, 2, 17, 11, 24, 2, 234567000));
    expectedRow.put(
        "timestamp", ZonedDateTime.of(2019, 2, 17, 11, 24, 1, 123000000, ZoneOffset.UTC));
    expectedRow.put("array_of_int64", List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L));
    assertThatPipeline(info).isRunning();
    assertThatResult(
            pipelineOperator()
                .waitForConditionAndCancel(createConfig(info), generateChecks(expectedRow)))
        .meetsConditions();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void supportsBigQueryMapsForCypherTarget() throws Exception {
    TableId table =
        bigQueryClient.createTable(
            testName,
            Schema.of(
                Field.newBuilder(
                        "map_column",
                        StandardSQLTypeName.STRUCT,
                        Field.newBuilder("name", StandardSQLTypeName.STRING).build(),
                        Field.newBuilder("age", StandardSQLTypeName.INT64).build())
                    .build()));
    Map<String, Object> sourceRow = new HashMap<>();
    sourceRow.put("map_column", Map.of("name", "john", "age", 24));
    bigQueryClient.write(testName, List.of(RowToInsert.of(sourceRow)));
    gcsClient.createArtifact(
        "spec.json", contentOf("/testing-specs/data-conversion/bq-spec-map.json"));
    gcsClient.createArtifact(
        "neo4j.json",
        String.format(
            "{\n"
                + "  \"server_url\": \"%s\",\n"
                + "  \"database\": \"%s\",\n"
                + "  \"auth_type\": \"basic\",\n"
                + "  \"username\": \"neo4j\",\n"
                + "  \"pwd\": \"%s\"\n"
                + "}",
            neo4jClient.getUri(), neo4jClient.getDatabaseName(), neo4jClient.getAdminPassword()));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jobSpecUri", getGcsPath("spec.json"))
            .addParameter("neo4jConnectionUri", getGcsPath("neo4j.json"))
            .addParameter(
                "optionsJson", String.format("{\"bqtable\": \"%s\"}", toTableSpecStandard(table)));
    LaunchInfo info = launchTemplate(options);

    Map<String, Object> expectedRow = new HashMap<>();
    expectedRow.put("name", "john");
    expectedRow.put("age", 24L);
    assertThatPipeline(info).isRunning();
    assertThatResult(
            pipelineOperator()
                .waitForConditionAndCancel(createConfig(info), generateChecks(expectedRow)))
        .meetsConditions();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void supportsMappedTypesForInlineCsv() throws Exception {
    gcsClient.createArtifact(
        "spec.json", contentOf("/testing-specs/data-conversion/inlinecsv-spec.json"));
    gcsClient.createArtifact(
        "neo4j.json",
        String.format(
            "{\n"
                + "  \"server_url\": \"%s\",\n"
                + "  \"database\": \"%s\",\n"
                + "  \"auth_type\": \"basic\",\n"
                + "  \"username\": \"neo4j\",\n"
                + "  \"pwd\": \"%s\"\n"
                + "}",
            neo4jClient.getUri(), neo4jClient.getDatabaseName(), neo4jClient.getAdminPassword()));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jobSpecUri", getGcsPath("spec.json"))
            .addParameter("neo4jConnectionUri", getGcsPath("neo4j.json"));
    LaunchInfo info = launchTemplate(options);

    Map<String, Object> expectedRow = new HashMap<>();
    expectedRow.put("int64", 50L);
    expectedRow.put("float64", 40.0D);
    expectedRow.put("string", "a string");
    expectedRow.put(
        "datetime_zone",
        ZonedDateTime.of(2020, 5, 1, 23, 59, 59, 999999999, ZoneId.of("Europe/Istanbul")));
    expectedRow.put(
        "datetime_offset",
        ZonedDateTime.of(2020, 5, 1, 23, 59, 59, 999999999, ZoneOffset.ofHours(3)));
    expectedRow.put("boolean", true);
    expectedRow.put("bytes", "Hello World".getBytes(StandardCharsets.UTF_8));
    assertThatPipeline(info).isRunning();

    assertThatResult(
            pipelineOperator()
                .waitForConditionAndCancel(createConfig(info), generateChecks(expectedRow)))
        .meetsConditions();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void supportsMappedTypesForExternalCsv() throws Exception {
    gcsClient.createArtifact(
        "external.csv", contentOf("/testing-specs/data-conversion/external.csv"));
    gcsClient.createArtifact(
        "spec.json", contentOf("/testing-specs/data-conversion/externalcsv-spec.json"));
    gcsClient.createArtifact(
        "neo4j.json",
        String.format(
            "{\n"
                + "  \"server_url\": \"%s\",\n"
                + "  \"database\": \"%s\",\n"
                + "  \"auth_type\": \"basic\",\n"
                + "  \"username\": \"neo4j\",\n"
                + "  \"pwd\": \"%s\"\n"
                + "}",
            neo4jClient.getUri(), neo4jClient.getDatabaseName(), neo4jClient.getAdminPassword()));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jobSpecUri", getGcsPath("spec.json"))
            .addParameter("neo4jConnectionUri", getGcsPath("neo4j.json"))
            .addParameter(
                "optionsJson",
                String.format("{\"externalcsvuri\": \"%s\"}", getGcsPath("external.csv")));

    LaunchInfo info = launchTemplate(options);

    Map<String, Object> expectedRow = new HashMap<>();
    expectedRow.put("int64", 50L);
    expectedRow.put("float64", 40.0D);
    expectedRow.put("string", "a string");
    expectedRow.put(
        "datetime_zone",
        ZonedDateTime.of(2020, 5, 1, 23, 59, 59, 999999999, ZoneId.of("Europe/Istanbul")));
    expectedRow.put(
        "datetime_offset",
        ZonedDateTime.of(2020, 5, 1, 23, 59, 59, 999999999, ZoneOffset.ofHours(3)));
    expectedRow.put("boolean", true);
    expectedRow.put("bytes", "Hello World".getBytes(StandardCharsets.UTF_8));
    assertThatPipeline(info).isRunning();

    assertThatResult(
            pipelineOperator()
                .waitForConditionAndCancel(createConfig(info), generateChecks(expectedRow)))
        .meetsConditions();
  }

  @SuppressWarnings("rawtypes")
  @NotNull
  private Supplier[] generateChecks(Map<String, Object> expectedRow) {
    return expectedRow.entrySet().stream()
        .map(
            e ->
                new ConditionCheck() {
                  @Override
                  protected @UnknownKeyFor @NonNull @Initialized String getDescription() {
                    return "check " + e.getKey();
                  }

                  @Override
                  protected CheckResult check() {
                    var result =
                        neo4jClient.run(
                            String.format("MATCH (n) RETURN n.`%s` AS prop", e.getKey()));
                    if (result.isEmpty()) {
                      return new CheckResult(false, "not persisted yet");
                    }

                    var actual = result.get(0).get("prop");
                    var expected = e.getValue();

                    return new CheckResult(
                        Objects.deepEquals(actual, expected),
                        String.format("Expected '%s' to deep equal '%s'", actual, expected));
                  }
                })
        .toArray(Supplier[]::new);
  }

  private String contentOf(String resourcePath) throws IOException {
    try (BufferedReader bufferedReader =
        new BufferedReader(
            new InputStreamReader(this.getClass().getResourceAsStream(resourcePath)))) {
      return bufferedReader.lines().collect(Collectors.joining("\n"));
    }
  }
}
