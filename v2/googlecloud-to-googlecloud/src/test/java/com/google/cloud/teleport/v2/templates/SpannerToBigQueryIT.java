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

import static org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(SpannerToBigQuery.class)
@RunWith(JUnit4.class)
public class SpannerToBigQueryIT extends TemplateTestBase {

  private static final int MESSAGES_COUNT = 20;

  private SpannerResourceManager spannerClient;
  private BigQueryResourceManager bigQueryClient;

  private String bigQuerySchema =
      "{\n"
          + "    \"fields\": [\n"
          + "        {\n"
          + "            \"name\": \"Id\",\n"
          + "            \"type\": \"INT64\"\n"
          + "        },\n"
          + "        {\n"
          + "            \"name\": \"FirstName\",\n"
          + "            \"type\": \"STRING\"\n"
          + "        },\n"
          + "        {\n"
          + "            \"name\": \"LastName\",\n"
          + "            \"type\": \"STRING\"\n"
          + "        }\n"
          + "    ]\n"
          + "}";

  @Before
  public void setup() throws IOException {
    spannerClient =
        SpannerResourceManager.builder(testName, PROJECT, REGION).maybeUseStaticInstance().build();
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    gcsClient.createArtifact("input/bq-schema.json", bigQuerySchema);
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(spannerClient, bigQueryClient);
  }

  @Test
  public void testSpannerToBigQuery() throws IOException {
    // Arrange
    String createTableStatement =
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  FirstName String(1024),\n"
                + "  LastName String(1024),\n"
                + ") PRIMARY KEY(Id)",
            testName);
    spannerClient.executeDdlStatement(createTableStatement);

    List<Mutation> expectedData = generateTableRows(testName);
    spannerClient.write(expectedData);

    String dataset = bigQueryClient.createDataset(REGION);
    String table = String.format("%s:%s.test-table", PROJECT, dataset);

    // Act
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("spannerInstanceId", spannerClient.getInstanceId())
            .addParameter("spannerDatabaseId", spannerClient.getDatabaseId())
            .addParameter("spannerTableId", testName)
            .addParameter("sqlQuery", "select * from " + testName)
            .addParameter("outputTableSpec", table)
            .addParameter("bigQuerySchemaPath", getGcsPath("input/bq-schema.json"));

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    TableResult records = bigQueryClient.readTable("test-table");

    List<Map<String, Object>> expectedRecords = new ArrayList<>();
    expectedData.forEach(
        mutation -> {
          Map<String, Object> expectedRecord =
              mutation.asMap().entrySet().stream()
                  .collect(
                      Collectors.toMap(
                          e -> e.getKey(),
                          // Only checking for int64 and string here. If adding another type, this
                          // will need to be fixed.
                          e ->
                              e.getValue().getType() == Type.int64()
                                  ? e.getValue().getInt64()
                                  : e.getValue().getString()));
          expectedRecords.add(expectedRecord);
        });

    assertThatBigQueryRecords(records).hasRecordsUnordered(expectedRecords);
  }

  @Test
  public void testSpannerToBigQueryNoSchemaFile() throws IOException {
    // Arrange
    String createTableStatement =
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  FirstName String(1024),\n"
                + "  LastName String(1024),\n"
                + ") PRIMARY KEY(Id)",
            testName);
    spannerClient.executeDdlStatement(createTableStatement);

    List<Mutation> expectedData = generateTableRows(testName);
    spannerClient.write(expectedData);

    List<Field> bqSchemaFields =
        Arrays.asList(
            Field.of("Id", StandardSQLTypeName.INT64),
            Field.of("FirstName", StandardSQLTypeName.STRING),
            Field.of("LastName", StandardSQLTypeName.STRING));
    Schema bqSchema = Schema.of(bqSchemaFields);

    TableId table = bigQueryClient.createTable("test-table", bqSchema);

    // Act
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("spannerInstanceId", spannerClient.getInstanceId())
            .addParameter("spannerDatabaseId", spannerClient.getDatabaseId())
            .addParameter("spannerTableId", testName)
            .addParameter("sqlQuery", "select * from " + testName)
            .addParameter("createDisposition", "CREATE_NEVER")
            .addParameter("outputTableSpec", toTableSpecLegacy(table));

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    TableResult records = bigQueryClient.readTable(table);

    List<Map<String, Object>> expectedRecords = new ArrayList<>();
    expectedData.forEach(
        mutation -> {
          Map<String, Object> expectedRecord =
              mutation.asMap().entrySet().stream()
                  .collect(
                      Collectors.toMap(
                          e -> e.getKey(),
                          // Only checking for int64 and string here. If adding another type, this
                          // will need to be fixed.
                          e ->
                              e.getValue().getType() == Type.int64()
                                  ? e.getValue().getInt64()
                                  : e.getValue().getString()));
          expectedRecords.add(expectedRecord);
        });

    assertThatBigQueryRecords(records).hasRecordsUnordered(expectedRecords);
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
}
