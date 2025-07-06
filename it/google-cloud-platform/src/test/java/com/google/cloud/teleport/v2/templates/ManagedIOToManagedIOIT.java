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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.bigquery.BigQueryResourceManagerUtils.toTableSpec;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link ManagedIOToManagedIO}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(ManagedIOToManagedIO.class)
@RunWith(JUnit4.class)
public final class ManagedIOToManagedIOIT extends TemplateTestBase {

  private static final Schema BQ_SCHEMA =
      Schema.of(
          Field.of("id", StandardSQLTypeName.INT64),
          Field.of("name", StandardSQLTypeName.STRING),
          Field.of("price", StandardSQLTypeName.FLOAT64));

  private BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setUp() {
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryResourceManager);
  }

  @Test
  public void testBigQueryToBigQuery() throws IOException {
    // Create source and sink tables
    TableId sourceTable = bigQueryResourceManager.createTable(testName + "_source", BQ_SCHEMA);
    TableId sinkTable = bigQueryResourceManager.createTable(testName + "_sink", BQ_SCHEMA);

    // Populate source table with test data
    List<RowToInsert> testData = new ArrayList<>();
    testData.add(RowToInsert.of(ImmutableMap.of("id", 1, "name", "Apple", "price", 1.0)));
    testData.add(RowToInsert.of(ImmutableMap.of("id", 2, "name", "Orange", "price", 1.5)));
    testData.add(RowToInsert.of(ImmutableMap.of("id", 3, "name", "Banana", "price", 0.5)));
    bigQueryResourceManager.write(testName + "_source", testData);

    // Configure source and sink
    Map<String, String> sourceConfig = new HashMap<>();
    sourceConfig.put("table", toTableSpec(PROJECT, sourceTable));

    Map<String, String> sinkConfig = new HashMap<>();
    sinkConfig.put("table", toTableSpec(PROJECT, sinkTable));

    // Launch the pipeline
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("sourceConnectorType", "BIGQUERY")
            .addParameter("sourceConfig", new Gson().toJson(sourceConfig))
            .addParameter("sinkConnectorType", "BIGQUERY")
            .addParameter("sinkConfig", new Gson().toJson(sinkConfig));

    LaunchInfo info = launchTemplate(options);
    // Wait for the pipeline to finish and check the results
    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                BigQueryRowsCheck.builder(bigQueryResourceManager, sinkTable)
                    .setMinRows(testData.size())
                    .build());

    assertThat(result).isEqualTo(Result.LAUNCH_FINISHED);
    assertThat(bigQueryResourceManager.getRowCount(sinkTable.getTable())).isEqualTo(testData.size());
  }
}
