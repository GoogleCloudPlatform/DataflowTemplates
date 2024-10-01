/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.templates;

import static org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.IOException;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link CSVToBigQuery} . */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(CSVToBigQuery.class)
@RunWith(JUnit4.class)
public final class CSVToBigQueryIT extends TemplateTestBase {

  private static final String BQ_DESTINATION_TABLE_SCHEMA_PATH =
      "CSVToBigQueryIT/bq_destination_table_schema.json";
  private static final String CSV_INPUT_WITHOUT_HEADER_PATH =
      "CSVToBigQueryIT/csv_input_without_header.csv";
  private static final String CSV_INPUT_WITH_HEADER_PATH =
      "CSVToBigQueryIT/csv_input_with_header.csv";
  private static final String CSV_COMPRESSED_IN_BZ2_INPUT_WITHOUT_HEADER_PATH =
      "CSVToBigQueryIT/csv_input_without_header.csv.bz2";
  private static final String CSV_COMPRESSED_IN_BZ2_INPUT_WITH_HEADER_PATH =
      "CSVToBigQueryIT/csv_input_with_header.csv.bz2";

  private BigQueryResourceManager bigQueryClient;

  @Before
  public void setup() throws IOException {
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryClient);
  }

  @Test
  public void testCSVWithoutHeaderToBigQuery() throws IOException {
    String schemaPath = "schema_for_csv_without_header.json";
    String inputPath = "csv_without_header.csv";
    // Arrange
    gcsClient.uploadArtifact(
        schemaPath, Resources.getResource(BQ_DESTINATION_TABLE_SCHEMA_PATH).getPath());
    gcsClient.uploadArtifact(
        inputPath, Resources.getResource(CSV_INPUT_WITHOUT_HEADER_PATH).getPath());

    bigQueryClient.createDataset(REGION);
    String bqDestinationTable = testName + "_without_header_destination";
    TableId destinationTable =
        bigQueryClient.createTable(
            bqDestinationTable,
            Schema.of(
                Field.of("book_id", StandardSQLTypeName.INT64),
                Field.of("title", StandardSQLTypeName.STRING)));

    String bqBadRecordsTable = testName + "_without_header_bad_records";
    TableId badRecordsTable =
        bigQueryClient.createTable(
            bqBadRecordsTable,
            Schema.of(
                Field.of("RawContent", StandardSQLTypeName.STRING),
                Field.of("ErrorMsg", StandardSQLTypeName.STRING)));

    // Act
    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("schemaJSONPath", getGcsPath(schemaPath))
                .addParameter("inputFilePattern", getGcsPath(inputPath))
                .addParameter("outputTable", toTableSpecLegacy(destinationTable))
                .addParameter("badRecordsOutputTable", toTableSpecLegacy(badRecordsTable))
                .addParameter("csvFormat", "Default")
                .addParameter("delimiter", "|")
                .addParameter("containsHeaders", "false")
                .addParameter(
                    "bigQueryLoadingTemporaryDirectory", getGcsPath("without-header-bq-tmp")));
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();
    TableResult tableRows = bigQueryClient.readTable(destinationTable);
    assertThatBigQueryRecords(tableRows)
        .hasRecordUnordered(ImmutableMap.of("book_id", 123, "title", "ABC"));
  }

  @Test
  public void testCSVWithHeaderToBigQuery() throws IOException {
    String schemaPath = "schema_for_csv_with_header.json";
    String inputPath = "csv_wit_header.csv";
    // Arrange
    gcsClient.uploadArtifact(
        schemaPath, Resources.getResource(BQ_DESTINATION_TABLE_SCHEMA_PATH).getPath());
    gcsClient.uploadArtifact(
        inputPath, Resources.getResource(CSV_INPUT_WITH_HEADER_PATH).getPath());

    bigQueryClient.createDataset(REGION);
    String bqDestinationTable = testName + "_with_header_destination";
    TableId destinationTable =
        bigQueryClient.createTable(
            bqDestinationTable,
            Schema.of(
                Field.of("book_id", StandardSQLTypeName.INT64),
                Field.of("title", StandardSQLTypeName.STRING)));

    String bqBadRecordsTable = testName + "_with_header_bad_records";
    TableId badRecordsTable =
        bigQueryClient.createTable(
            bqBadRecordsTable,
            Schema.of(
                Field.of("RawContent", StandardSQLTypeName.STRING),
                Field.of("ErrorMsg", StandardSQLTypeName.STRING)));

    // Act
    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("schemaJSONPath", getGcsPath(schemaPath))
                .addParameter("inputFilePattern", getGcsPath(inputPath))
                .addParameter("outputTable", toTableSpecLegacy(destinationTable))
                .addParameter("badRecordsOutputTable", toTableSpecLegacy(badRecordsTable))
                .addParameter("csvFormat", "Default")
                .addParameter("delimiter", "|")
                .addParameter("containsHeaders", "true")
                .addParameter(
                    "bigQueryLoadingTemporaryDirectory", getGcsPath("with-header-bq-tmp")));
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();
    TableResult tableRows = bigQueryClient.readTable(destinationTable);
    assertThatBigQueryRecords(tableRows)
        .hasRecordUnordered(ImmutableMap.of("book_id", 123, "title", "ABC"));
  }

  @Test
  public void testCSVCompressedInBz2WithoutHeaderToBigQuery() throws IOException {
    String schemaPath = "schema_for_csv_in_bz2_without_header.json";
    String inputPath = "csv_without_header.csv.bz2";
    // Arrange
    gcsClient.uploadArtifact(
        schemaPath, Resources.getResource(BQ_DESTINATION_TABLE_SCHEMA_PATH).getPath());
    gcsClient.uploadArtifact(
        inputPath,
        Resources.getResource(CSV_COMPRESSED_IN_BZ2_INPUT_WITHOUT_HEADER_PATH).getPath());

    bigQueryClient.createDataset(REGION);
    String bqDestinationTable = testName + "_bz2_without_header_destination";
    TableId destinationTable =
        bigQueryClient.createTable(
            bqDestinationTable,
            Schema.of(
                Field.of("book_id", StandardSQLTypeName.INT64),
                Field.of("title", StandardSQLTypeName.STRING)));

    String bqBadRecordsTable = testName + "_bz2_without_header_bad_records";
    TableId badRecordsTable =
        bigQueryClient.createTable(
            bqBadRecordsTable,
            Schema.of(
                Field.of("RawContent", StandardSQLTypeName.STRING),
                Field.of("ErrorMsg", StandardSQLTypeName.STRING)));

    // Act
    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("schemaJSONPath", getGcsPath(schemaPath))
                .addParameter("inputFilePattern", getGcsPath(inputPath))
                .addParameter("outputTable", toTableSpecLegacy(destinationTable))
                .addParameter("badRecordsOutputTable", toTableSpecLegacy(badRecordsTable))
                .addParameter("csvFormat", "Default")
                .addParameter("delimiter", "|")
                .addParameter("containsHeaders", "false")
                .addParameter(
                    "bigQueryLoadingTemporaryDirectory", getGcsPath("bz2-without-header-bq-tmp")));
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();
    TableResult tableRows = bigQueryClient.readTable(destinationTable);
    assertThatBigQueryRecords(tableRows)
        .hasRecordUnordered(ImmutableMap.of("book_id", 123, "title", "ABC"));
  }

  @Test
  public void testCSVCompressedInBz2WithHeaderToBigQuery() throws IOException {
    String schemaPath = "schema_for_csv_with_header.json";
    String inputPath = "csv_wit_header.csv.bz2";
    // Arrange
    gcsClient.uploadArtifact(
        schemaPath, Resources.getResource(BQ_DESTINATION_TABLE_SCHEMA_PATH).getPath());
    gcsClient.uploadArtifact(
        inputPath, Resources.getResource(CSV_COMPRESSED_IN_BZ2_INPUT_WITH_HEADER_PATH).getPath());

    bigQueryClient.createDataset(REGION);
    String bqDestinationTable = testName + "_bz2_with_header_destination";
    TableId destinationTable =
        bigQueryClient.createTable(
            bqDestinationTable,
            Schema.of(
                Field.of("book_id", StandardSQLTypeName.INT64),
                Field.of("title", StandardSQLTypeName.STRING)));

    String bqBadRecordsTable = testName + "_bz2_with_header_bad_records";
    TableId badRecordsTable =
        bigQueryClient.createTable(
            bqBadRecordsTable,
            Schema.of(
                Field.of("RawContent", StandardSQLTypeName.STRING),
                Field.of("ErrorMsg", StandardSQLTypeName.STRING)));

    // Act
    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("schemaJSONPath", getGcsPath(schemaPath))
                .addParameter("inputFilePattern", getGcsPath(inputPath))
                .addParameter("outputTable", toTableSpecLegacy(destinationTable))
                .addParameter("badRecordsOutputTable", toTableSpecLegacy(badRecordsTable))
                .addParameter("csvFormat", "Default")
                .addParameter("delimiter", "|")
                .addParameter("containsHeaders", "true")
                .addParameter(
                    "bigQueryLoadingTemporaryDirectory", getGcsPath("bz2-with-header-bq-tmp")));
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();
    TableResult tableRows = bigQueryClient.readTable(destinationTable);
    assertThatBigQueryRecords(tableRows)
        .hasRecordUnordered(ImmutableMap.of("book_id", 123, "title", "ABC"));
  }
}
