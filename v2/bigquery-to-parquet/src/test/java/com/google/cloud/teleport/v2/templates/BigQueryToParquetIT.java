/*
 * Copyright (C) 2022 Google LLC
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
import static org.apache.beam.it.common.TestProperties.getProperty;
import static org.apache.beam.it.gcp.artifacts.matchers.ArtifactAsserts.assertThatArtifacts;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.Tuple;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.utils.BigQueryTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link BigQueryToParquet}. */
// SkipDirectRunnerTest: BigQueryIO fails mutation check https://github.com/apache/beam/issues/25319
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(BigQueryToParquet.class)
@RunWith(JUnit4.class)
public final class BigQueryToParquetIT extends TemplateTestBase {

  private BigQueryResourceManager bigQueryResourceManager;

  // Define a set of parameters used to allow configuration of the test size being run.
  private static final String BIGQUERY_ID_COL = "test_id";
  private static final int BIGQUERY_NUM_ROWS =
      Integer.parseInt(getProperty("numRows", "100", TestProperties.Type.PROPERTY));
  private static final int BIGQUERY_NUM_FIELDS =
      Integer.parseInt(getProperty("numFields", "50", TestProperties.Type.PROPERTY));
  private static final int BIGQUERY_MAX_ENTRY_LENGTH =
      Integer.min(
          300, Integer.parseInt(getProperty("maxEntryLength", "20", TestProperties.Type.PROPERTY)));

  // Parameters specific to testing time partitioned tables
  private static final String PARTITION_FIELD = "timePartitionCol";
  private static final String ROW_RESTRICTION =
      "TIMESTAMP_TRUNC(timePartitionCol, HOUR) = TIMESTAMP(\"2025-03-06T15:00:00\")";
  private static final List<String> timestamps =
      List.of(
          "2025-03-06T14:30:00Z", // Hour 14 (should NOT appear in results)
          "2025-03-06T15:00:00Z", // Hour 15 (expected in query results)
          "2025-03-06T15:45:00Z", // Hour 15 (expected in query results)
          "2025-03-06T16:00:00Z" // Hour 16 (should NOT appear in results)
          );
  private static final List<String> expectedTimestamps =
      List.of("2025-03-06T15:00:00Z", "2025-03-06T15:45:00Z");

  @Before
  public void setup() {
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryResourceManager);
  }

  @Test
  public void testBigQueryToParquet() throws IOException {
    // Arrange
    Tuple<Schema, List<RowToInsert>> generatedTable =
        BigQueryTestUtil.generateBigQueryTable(
            BIGQUERY_ID_COL, BIGQUERY_NUM_ROWS, BIGQUERY_NUM_FIELDS, BIGQUERY_MAX_ENTRY_LENGTH);
    Schema bigQuerySchema = generatedTable.x();
    List<RowToInsert> bigQueryRows = generatedTable.y();
    TableId table = bigQueryResourceManager.createTable(testName, bigQuerySchema);
    bigQueryResourceManager.write(testName, bigQueryRows);
    Pattern expectedFilePattern = Pattern.compile(".*");

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("tableRef", toTableSpecLegacy(table))
            .addParameter("bucket", getGcsPath(testName))
            .addParameter("numShards", "5");

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> artifacts = gcsClient.listArtifacts(testName, expectedFilePattern);
    assertThat(artifacts).hasSize(5);
    assertThatArtifacts(artifacts)
        .asParquetRecords()
        .hasRecordsUnordered(
            bigQueryRows.stream().map(RowToInsert::getContent).collect(Collectors.toList()));
  }

  @Test
  public void testTimePartitionedBigQueryTableToParquet() throws IOException {
    // Arrange
    Tuple<Schema, List<RowToInsert>> generatedTable =
        BigQueryTestUtil.generateBigQueryTable(
            BIGQUERY_ID_COL, BIGQUERY_NUM_ROWS, BIGQUERY_NUM_FIELDS, BIGQUERY_MAX_ENTRY_LENGTH);

    // Ensure the partitioned column is present in the schema
    List<Field> fields = new ArrayList<>(generatedTable.x().getFields());
    fields.add(Field.of(PARTITION_FIELD, StandardSQLTypeName.TIMESTAMP));
    Schema bigQuerySchema = Schema.of(fields);

    // Auto-generated test data
    List<RowToInsert> generatedBigQueryRows = generatedTable.y();
    // Final data to insert to test table
    List<RowToInsert> bigQueryRows = new ArrayList<>();
    // Expected test data
    List<RowToInsert> expectedBigQueryRows = new ArrayList<>();

    // Insert test data to the partitioned column
    for (int i = 0; i < generatedBigQueryRows.size(); i++) {
      RowToInsert generatedRow = generatedBigQueryRows.get(i);
      Map<String, Object> content = new HashMap<>(generatedRow.getContent());

      String timestampString = timestamps.get(i % timestamps.size());
      content.put(PARTITION_FIELD, timestampString);

      bigQueryRows.add(RowToInsert.of(content));

      // prep expected dataset used for assertion, we expect timestamps that satisfy ROW_RESTRICTION
      if (expectedTimestamps.contains(timestampString)) {
        // Convert to Epoch since timestamps stored in Parquet files are in Epoch
        Instant instant = Instant.parse(timestampString);
        long epochMillis = instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
        Map<String, Object> expectedContent = new HashMap<>(generatedRow.getContent());
        expectedContent.put(PARTITION_FIELD, epochMillis);
        expectedBigQueryRows.add(RowToInsert.of(expectedContent));
      }
    }

    // Set up time-partitioned test table
    TimePartitioning timePartitioning =
        TimePartitioning.newBuilder(TimePartitioning.Type.HOUR).setField(PARTITION_FIELD).build();
    TableId table =
        bigQueryResourceManager.createTimePartitionedTable(
            testName, bigQuerySchema, timePartitioning);
    bigQueryResourceManager.write(testName, bigQueryRows);

    Pattern expectedFilePattern = Pattern.compile(".*");

    // Configure pipeline launch
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("tableRef", toTableSpecLegacy(table))
            .addParameter("bucket", getGcsPath(testName))
            .addParameter("numShards", "5")
            .addParameter("rowRestriction", ROW_RESTRICTION);

    // Act: Launch pipeline
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> artifacts = gcsClient.listArtifacts(testName, expectedFilePattern);
    assertThat(artifacts).hasSize(5);
    assertThatArtifacts(artifacts)
        .asParquetRecords()
        .hasRecordsUnordered(
            expectedBigQueryRows.stream()
                .map(RowToInsert::getContent)
                .collect(Collectors.toList()));
  }
}
