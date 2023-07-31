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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.artifacts.matchers.ArtifactAsserts.assertThatArtifacts;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.artifacts.utils.AvroTestUtil;
import org.apache.beam.it.gcp.artifacts.utils.ParquetTestUtil;
import org.apache.beam.it.truthmatchers.RecordsSubject;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link FileFormatConversion}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(FileFormatConversion.class)
@RunWith(JUnit4.class)
public final class FileFormatConversionIT extends TemplateTestBase {

  private static final String SCHEMA_JSON =
      "{\n"
          + "  \"type\" : \"record\",\n"
          + "  \"name\" : \"test_file\",\n"
          + "  \"namespace\" : \"com.test.schema\",\n"
          + "  \"fields\" : [\n"
          + "    {\n"
          + "      \"name\": \"id\",\n"
          + "      \"type\": \"string\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"state\",\n"
          + "      \"type\": \"string\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"price\",\n"
          + "      \"type\": \"double\"\n"
          + "    }\n"
          + "  ]\n"
          + "}";
  private static final Schema SCHEMA = SchemaUtils.parseAvroSchema(SCHEMA_JSON);

  @Before
  public void setUp() {
    gcsClient.createArtifact("input/schema.json", SCHEMA_JSON);
  }

  @Test
  public void testCsvToAvro() throws IOException {
    this.baseFromCsv(
        "avro", (artifacts, schema) -> assertThatArtifacts(artifacts).asAvroRecords(schema));
  }

  @Test
  public void testCsvToParquet() throws IOException {
    this.baseFromCsv(
        "parquet", (artifacts, schema) -> assertThatArtifacts(artifacts).asParquetRecords());
  }

  public void baseFromCsv(
      String toFormat, BiFunction<List<Artifact>, Schema, RecordsSubject> parseFunction)
      throws IOException {
    // Arrange
    gcsClient.createArtifact(
        "input/csv_file.csv",
        "id|state|price\n"
            + "007|CA|26.23\n"
            + "008|NC|30.22\n"
            + "009|WA|31.22\n"
            + "010|NY|29.33\n"
            + "011|KY|30.01\n"
            + "012|MD|35.1\n"
            + "013|VA|35.11\n"
            + "014|OH|29.82\n"
            + "015|DE|27.84\n"
            + "016|MN|25.37\n");

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputFileFormat", "csv")
            .addParameter("outputFileFormat", toFormat)
            .addParameter("inputFileSpec", getGcsPath("input/*.csv"))
            .addParameter("containsHeaders", "true")
            .addParameter("delimiter", "|")
            .addParameter("outputBucket", getGcsPath("output/"))
            .addParameter("schema", getGcsPath("input/schema.json"))
            .addParameter("outputFilePrefix", testName);

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> artifacts =
        gcsClient.listArtifacts("output/", Pattern.compile(".*" + testName + ".*\\." + toFormat));
    assertThat(artifacts).isNotEmpty();
    parseFunction.apply(artifacts, SCHEMA).hasRecordsUnordered(buildExpectedRows());
  }

  @Test
  public void testAvroToParquet() throws IOException {
    // Arrange
    gcsClient.createArtifact(
        "input/data.avro", AvroTestUtil.createAvroFile(SCHEMA, createTestGenericRecords()));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputFileFormat", "avro")
            .addParameter("outputFileFormat", "parquet")
            .addParameter("inputFileSpec", getGcsPath("input/*.avro"))
            .addParameter("outputBucket", getGcsPath("output/"))
            .addParameter("schema", getGcsPath("input/schema.json"))
            .addParameter("outputFilePrefix", testName);

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> artifacts =
        gcsClient.listArtifacts("output/", Pattern.compile(".*" + testName + ".*\\.parquet"));
    assertThat(artifacts).isNotEmpty();
    assertThatArtifacts(artifacts).asParquetRecords().hasRecordsUnordered(buildExpectedRows());
  }

  @Test
  public void testParquetToAvro() throws IOException {
    // Arrange
    gcsClient.createArtifact(
        "input/data.parquet",
        ParquetTestUtil.createParquetFile(SCHEMA, createTestGenericRecords()));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputFileFormat", "parquet")
            .addParameter("outputFileFormat", "avro")
            .addParameter("inputFileSpec", getGcsPath("input/*.parquet"))
            .addParameter("outputBucket", getGcsPath("output/"))
            .addParameter("schema", getGcsPath("input/schema.json"))
            .addParameter("outputFilePrefix", testName);

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> artifacts =
        gcsClient.listArtifacts("output/", Pattern.compile(".*" + testName + ".*\\.avro"));
    assertThat(artifacts).isNotEmpty();
    assertThatArtifacts(artifacts).asAvroRecords(SCHEMA).hasRecordsUnordered(buildExpectedRows());
  }

  private List<GenericRecord> createTestGenericRecords() {
    return buildExpectedRows().stream()
        .map(
            row ->
                new GenericRecordBuilder(SCHEMA)
                    .set("id", row.get("id"))
                    .set("state", row.get("state"))
                    .set("price", Double.parseDouble(row.get("price").toString()))
                    .build())
        .collect(Collectors.toList());
  }

  private List<Map<String, Object>> buildExpectedRows() {
    return List.of(
        Map.of("id", "007", "state", "CA", "price", "26.23"),
        Map.of("id", "008", "state", "NC", "price", "30.22"),
        Map.of("id", "009", "state", "WA", "price", "31.22"),
        Map.of("id", "010", "state", "NY", "price", "29.33"),
        Map.of("id", "011", "state", "KY", "price", "30.01"),
        Map.of("id", "012", "state", "MD", "price", "35.1"),
        Map.of("id", "013", "state", "VA", "price", "35.11"),
        Map.of("id", "014", "state", "OH", "price", "29.82"),
        Map.of("id", "015", "state", "DE", "price", "27.84"),
        Map.of("id", "016", "state", "MN", "price", "25.37"));
  }
}
