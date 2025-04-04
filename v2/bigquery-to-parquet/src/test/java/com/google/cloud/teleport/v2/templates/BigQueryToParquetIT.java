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
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.ArrayList;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
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
@Category({TemplateIntegrationTest.class})
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
  private static final String PARTITION_FIELD = "timePartitionCol";

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
    // Predefined timestamps for test data
    List<String> timestamps =
        List.of(
            "2025-03-06T14:30:00Z", // Hour 14 (should NOT appear in results)
            "2025-03-06T15:00:00Z", // Hour 15 (expected in query results)
            "2025-03-06T15:45:00Z", // Hour 15 (expected in query results)
            "2025-03-06T16:00:00Z" // Hour 16 (should NOT appear in results)
            );

    // Arrange
    Tuple<Schema, List<RowToInsert>> generatedTable =
        BigQueryTestUtil.generateBigQueryTable(
            BIGQUERY_ID_COL, BIGQUERY_NUM_ROWS, BIGQUERY_NUM_FIELDS, BIGQUERY_MAX_ENTRY_LENGTH);

    // Ensure the partition column is present in the schema
    List<Field> fields = new ArrayList<>(generatedTable.x().getFields());
    fields.add(Field.of(PARTITION_FIELD, StandardSQLTypeName.TIMESTAMP));
    Schema bigQuerySchema = Schema.of(fields);

    // Assign timestamps from the list
    List<RowToInsert> bigQueryRows = new ArrayList<>();
    for (int i = 0; i < generatedTable.y().size(); i++) {
      Map<String, Object> content = new HashMap<>(generatedTable.y().get(i).getContent());
      content.put(
          PARTITION_FIELD, timestamps.get(i % timestamps.size())); // Cycle through timestamps
      bigQueryRows.add(RowToInsert.of(generatedTable.y().get(i).getInsertId(), content));
    }

    // Set up partition test table
    TimePartitioning timePartitioning =
        TimePartitioning.newBuilder(TimePartitioning.Type.HOUR).setField(PARTITION_FIELD).build();
    TableId table =
        bigQueryResourceManager.createTimePartitionedTable(
            testName, bigQuerySchema, timePartitioning);
    bigQueryResourceManager.write(testName, bigQueryRows);

    //   Define expected test results
    Pattern expectedFilePattern = Pattern.compile(".*");

    List<RowToInsert> expectedPartitionRows =
        bigQueryRows.stream().filter(
            row -> {
              String timeStr = row.getContent().get(PARTITION_FIELD).toString();
              Instant timestamp = Instant.parse(timeStr);
              Instant truncatedTimestamp = timestamp.truncatedTo(ChronoUnit.HOURS);
              return truncatedTimestamp.equals(Instant.parse("2025-03-06T15:00:00Z"));
            }).collect(Collectors.toList());

    // Configure pipeline launch
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("tableRef", toTableSpecLegacy(table))
            .addParameter("bucket", getGcsPath(testName))
            .addParameter("numShards", "5")
            .addParameter(
                "rowRestriction",
                "TIMESTAMP_TRUNC(time, HOUR) = TIMESTAMP(\"2025-03-06T15:00:00\")");

    // Act: Launch pipeline
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    assertThat(true).equals(false);

    List<Artifact> artifacts = gcsClient.listArtifacts(testName, expectedFilePattern);
    assertThat(artifacts).hasSize(1);
    assertThatArtifacts(artifacts)
        .asParquetRecords()
        .hasRecordsUnordered(
            bigQueryRows.stream().map(RowToInsert::getContent).collect(Collectors.toList()));
  }
}
