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

import static com.google.cloud.teleport.v2.neo4j.templates.Connections.jsonBasicPayload;
import static com.google.cloud.teleport.v2.neo4j.templates.Resources.contentOf;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
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
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.neo4j.Neo4jResourceManager;
import org.apache.beam.it.neo4j.conditions.Neo4jQueryCheck;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(GoogleCloudToNeo4j.class)
@RunWith(JUnit4.class)
public class DataConversionIT extends TemplateTestBase {

  private static BigQueryResourceManager bigQueryClient;
  private static Neo4jResourceManager neo4jClient;

  @BeforeClass
  public static void setUpClass() {
    // Tests sharing an active neo4j test container
    neo4jClient =
        Neo4jResourceManager.builder(DataConversionIT.class.getName())
            .setAdminPassword("letmein!")
            .setHost(TestProperties.hostIp())
            .build();
  }

  @Before
  public void setUp() {
    synchronized (DataConversionIT.class) {
      if (bigQueryClient == null) {
        bigQueryClient =
            BigQueryResourceManager.builder(DataConversionIT.class.getName(), PROJECT, credentials)
                .build();
      }
    }
  }

  @After
  public void cleanUp() {
    neo4jClient.run("MATCH (n) DETACH DELETE n;");
  }

  @AfterClass
  public static void tearDownClass() {
    if (bigQueryClient == null) {
      ResourceManagerUtils.cleanResources(neo4jClient);
    } else {
      ResourceManagerUtils.cleanResources(bigQueryClient, neo4jClient);
    }
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

  @Test
  public void treatsEmptyCsvStringsAsNulls() throws IOException {
    gcsClient.createArtifact(
        "external.csv", contentOf("/testing-specs/data-conversion/empty-strings.csv"));
    gcsClient.createArtifact(
        "spec.json", contentOf("/testing-specs/data-conversion/empty-strings-spec.json"));
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

    assertThatPipeline(info).isRunning();
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(info),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery("MATCH (n:Node) RETURN n {.*} as properties")
                    .setExpectedResult(List.of(Map.of("properties", Map.of("int64", "1"))))
                    .build());
    assertThatResult(result).meetsConditions();
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

  @Test
  public void escapesFieldNamesForBigQuery() throws Exception {
    TableId table =
        bigQueryClient.createTable(
            testName,
            Schema.of(
                Field.newBuilder("select", StandardSQLTypeName.STRING).build(),
                Field.newBuilder("age", StandardSQLTypeName.INT64).build()));
    Map<String, Object> sourceRow = new HashMap<>();
    sourceRow.put("select", "john");
    sourceRow.put("age", 23L);
    bigQueryClient.write(testName, List.of(InsertAllRequest.RowToInsert.of(sourceRow)));
    gcsClient.createArtifact("spec.json", contentOf("/testing-specs/escaping/bq-spec.json"));
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
    expectedRow.put("select", "john");
    expectedRow.put("where", 1L);
    assertThatPipeline(info).isRunning();

    assertThatResult(
            pipelineOperator()
                .waitForConditionAndCancel(
                    createConfig(info),
                    Neo4jQueryCheck.builder(neo4jClient)
                        .setQuery("MATCH (n) RETURN labels(n) AS labels, properties(n) AS props")
                        .setExpectedResult(
                            List.of(Map.of("labels", List.of("Node"), "props", expectedRow)))
                        .build()))
        .meetsConditions();
  }

  @Test
  public void escapesFieldNamesForInlineCSV() throws Exception {
    gcsClient.createArtifact("spec.json", contentOf("/testing-specs/escaping/inlinecsv-spec.json"));
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
    expectedRow.put("select", "john");
    expectedRow.put("where", 1L);
    assertThatPipeline(info).isRunning();

    assertThatResult(
            pipelineOperator()
                .waitForConditionAndCancel(
                    createConfig(info),
                    Neo4jQueryCheck.builder(neo4jClient)
                        .setQuery("MATCH (n) RETURN labels(n) AS labels, properties(n) AS props")
                        .setExpectedResult(
                            List.of(Map.of("labels", List.of("Node"), "props", expectedRow)))
                        .build()))
        .meetsConditions();
  }

  @Test
  public void escapesFieldsNamesForExternalCSV() throws Exception {
    gcsClient.createArtifact("external.csv", contentOf("/testing-specs/escaping/external.csv"));
    gcsClient.createArtifact(
        "spec.json", contentOf("/testing-specs/escaping/externalcsv-spec.json"));
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
    expectedRow.put("select", "john");
    expectedRow.put("where", 1L);
    assertThatPipeline(info).isRunning();

    assertThatResult(
            pipelineOperator()
                .waitForConditionAndCancel(
                    createConfig(info),
                    Neo4jQueryCheck.builder(neo4jClient)
                        .setQuery("MATCH (n) RETURN labels(n) AS labels, properties(n) AS props")
                        .setExpectedResult(
                            List.of(Map.of("labels", List.of("Node"), "props", expectedRow)))
                        .build()))
        .meetsConditions();
  }

  @Test
  // TODO: generate bigquery data set once import-spec supports value interpolation
  public void importsStackoverflowUsers() throws IOException {
    String spec = contentOf("/testing-specs/synthetic-fields/spec.yml");
    gcsClient.createArtifact("spec.yml", spec);
    gcsClient.createArtifact("neo4j-connection.json", jsonBasicPayload(neo4jClient));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jobSpecUri", getGcsPath("spec.yml"))
            .addParameter("neo4jConnectionUri", getGcsPath("neo4j-connection.json"));
    LaunchInfo info = launchTemplate(options);

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(info),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery("MATCH (u:User) RETURN count(u) AS count")
                    .setExpectedResult(List.of(Map.of("count", 10L)))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "MATCH (l:Letter) WITH DISTINCT toUpper(l.char) AS char ORDER BY char ASC RETURN collect(char) AS chars")
                    .setExpectedResult(
                        List.of(Map.of("chars", List.of("A", "C", "G", "I", "J", "T", "W"))))
                    .build());
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void mapsBooleanPropertiesFromInlineTextSource() throws Exception {
    gcsClient.createArtifact(
        "spec.json", contentOf("/testing-specs/property-mappings/booleans-text-spec.json"));
    gcsClient.createArtifact("neo4j.json", jsonBasicPayload(neo4jClient));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jobSpecUri", getGcsPath("spec.json"))
            .addParameter("neo4jConnectionUri", getGcsPath("neo4j.json"));
    LaunchInfo info = launchTemplate(options);

    assertThatPipeline(info).isRunning();
    assertBooleansArePersisted(info);
  }

  @Test
  public void mapsBooleanPropertiesFromBigQuery() throws Exception {
    TableId table =
        bigQueryClient.createTable(
            testName,
            Schema.of(
                Field.newBuilder("id", StandardSQLTypeName.STRING).build(),
                Field.newBuilder("truthy", StandardSQLTypeName.BOOL).build()));
    bigQueryClient.write(
        testName,
        List.of(
            RowToInsert.of(Map.of("id", "bool-1", "truthy", false)),
            RowToInsert.of(Map.of("id", "bool-2", "truthy", true)),
            RowToInsert.of(Map.of("id", "bool-3", "truthy", true)),
            RowToInsert.of(Map.of("id", "bool-4", "truthy", false))));
    gcsClient.createArtifact(
        "spec.json", contentOf("/testing-specs/property-mappings/booleans-bq-spec.json"));
    gcsClient.createArtifact("neo4j.json", jsonBasicPayload(neo4jClient));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jobSpecUri", getGcsPath("spec.json"))
            .addParameter("neo4jConnectionUri", getGcsPath("neo4j.json"))
            .addParameter(
                "optionsJson", String.format("{\"bqtable\": \"%s\"}", toTableSpecStandard(table)));
    LaunchInfo info = launchTemplate(options);

    assertThatPipeline(info).isRunning();
    assertBooleansArePersisted(info);
  }

  @Test
  public void importsNodesWithLabelNamedSameAsSourceField() throws IOException {

    TableId table =
        bigQueryClient.createTable(
            testName,
            Schema.of(
                Field.newBuilder("Station", StandardSQLTypeName.STRING).build(),
                Field.newBuilder("Zone", StandardSQLTypeName.INT64).build(),
                Field.newBuilder("Latitude", StandardSQLTypeName.FLOAT64).build(),
                Field.newBuilder("Longitude", StandardSQLTypeName.FLOAT64).build()));
    bigQueryClient.write(
        testName,
        List.of(
            RowToInsert.of(
                Map.of(
                    "Station",
                    "station-1",
                    "Zone",
                    1,
                    "Latitude",
                    51.51434226,
                    "Longitude",
                    -0.075626912)),
            RowToInsert.of(
                Map.of(
                    "Station",
                    "station-2",
                    "Zone",
                    1,
                    "Latitude",
                    51.51434226,
                    "Longitude",
                    -0.075626912)),
            RowToInsert.of(
                Map.of(
                    "Station",
                    "station-3",
                    "Zone",
                    1,
                    "Latitude",
                    51.51434226,
                    "Longitude",
                    -0.075626912)),
            RowToInsert.of(
                Map.of(
                    "Station",
                    "station-4",
                    "Zone",
                    2,
                    "Latitude",
                    51.51434226,
                    "Longitude",
                    -0.075626912))));
    gcsClient.createArtifact(
        "spec.json", contentOf("/testing-specs/property-mappings/mapping-clash-bq-spec.json"));
    gcsClient.createArtifact("neo4j.json", jsonBasicPayload(neo4jClient));

    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("jobSpecUri", getGcsPath("spec.json"))
                .addParameter("neo4jConnectionUri", getGcsPath("neo4j.json"))
                .addParameter(
                    "optionsJson",
                    String.format("{\"bqtable\": \"%s\"}", toTableSpecStandard(table))));

    assertThatPipeline(info).isRunning();
    assertThatResult(
            pipelineOperator()
                .waitForConditionAndCancel(
                    createConfig(info),
                    Neo4jQueryCheck.builder(neo4jClient)
                        .setQuery("MATCH (n:Station) RETURN count(n) AS count")
                        .setExpectedResult(List.of(Map.of("count", 4L)))
                        .build()))
        .meetsConditions();
  }

  private void assertBooleansArePersisted(LaunchInfo info) throws IOException {
    assertThatResult(
            pipelineOperator()
                .waitForConditionAndCancel(
                    createConfig(info),
                    Neo4jQueryCheck.builder(neo4jClient)
                        .setQuery(
                            "MATCH (n) RETURN labels(n) AS labels, properties(n) AS props ORDER BY n.id ASC")
                        .setExpectedResult(
                            List.of(
                                Map.of(
                                    "labels",
                                    List.of("Boolean"),
                                    "props",
                                    Map.of("id", "bool-1", "truthy", false)),
                                Map.of(
                                    "labels",
                                    List.of("Boolean"),
                                    "props",
                                    Map.of("id", "bool-2", "truthy", true)),
                                Map.of(
                                    "labels",
                                    List.of("Boolean"),
                                    "props",
                                    Map.of("id", "bool-3", "truthy", true)),
                                Map.of(
                                    "labels",
                                    List.of("Boolean"),
                                    "props",
                                    Map.of("id", "bool-4", "truthy", false))))
                        .build()))
        .meetsConditions();
  }

  private String contentOf(String resourcePath) throws IOException {
    try (BufferedReader bufferedReader =
        new BufferedReader(
            new InputStreamReader(this.getClass().getResourceAsStream(resourcePath)))) {
      return bufferedReader.lines().collect(Collectors.joining("\n"));
    }
  }
}
