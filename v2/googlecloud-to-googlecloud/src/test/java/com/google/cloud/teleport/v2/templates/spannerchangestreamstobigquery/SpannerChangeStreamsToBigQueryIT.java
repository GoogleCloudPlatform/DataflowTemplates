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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Config;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ExceptionUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link SpannerChangeStreamsToBigQuery Spanner Change Streams to BigQuery}
 * template.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerChangeStreamsToBigQuery.class)
@RunWith(JUnit4.class)
public class SpannerChangeStreamsToBigQueryIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamsToBigQueryIT.class);

  private SpannerResourceManager spannerResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  private static final AtomicInteger counter = new AtomicInteger(0);

  private LaunchInfo launchInfo;

  @Before
  public void setup() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION).maybeUseStaticInstance().build();
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testSpannerChangeStreamsToBigQueryBasic() throws IOException {
    String spannerTable = testName + RandomStringUtils.randomAlphanumeric(1, 5);
    String createTableStatement =
        String.format(
            "CREATE TABLE %s (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  FirstName String(1024),\n"
                + "  LastName String(1024),\n"
                + ") PRIMARY KEY(Id)",
            spannerTable);

    String cdcTable = spannerTable + "_changelog";
    spannerResourceManager.executeDdlStatement(createTableStatement);
    String createChangeStreamStatement =
        String.format(
            "CREATE CHANGE STREAM %s_stream FOR %s OPTIONS (value_capture_type = 'NEW_ROW')",
            testName, spannerTable);
    spannerResourceManager.executeDdlStatement(createChangeStreamStatement);
    bigQueryResourceManager.createDataset(REGION);

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();

    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("spannerProjectId", PROJECT)
                    .addParameter("spannerInstanceId", spannerResourceManager.getInstanceId())
                    .addParameter("spannerDatabase", spannerResourceManager.getDatabaseId())
                    .addParameter(
                        "spannerMetadataInstanceId", spannerResourceManager.getInstanceId())
                    .addParameter("spannerMetadataDatabase", spannerResourceManager.getDatabaseId())
                    .addParameter("spannerChangeStreamName", testName + "_stream")
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("rpcPriority", "HIGH")
                    .addParameter("disableDlqRetries", "true")));

    assertThatPipeline(launchInfo).isRunning();

    int key = nextValue();
    String firstName = UUID.randomUUID().toString();
    String lastName = UUID.randomUUID().toString();
    List<Pair> colValPairs =
        Arrays.asList(new Pair("FirstName", firstName), new Pair("LastName", lastName));
    Mutation expectedData = generateTableRow(spannerTable, key, colValPairs);
    spannerResourceManager.write(Collections.singletonList(expectedData));
    String query = queryCdcTable(cdcTable, key);
    waitForQueryToReturnRows(query, 1, true);

    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    assertEquals(1, tableResult.getTotalRows());
    for (FieldValueList row : tableResult.iterateAll()) {
      assertTrue(validateChangeLogTableRow(row, colValPairs));
    }
  }

  boolean validateChangeLogTableRow(FieldValueList row, List<Pair> pairs) {
    for (Pair pair : pairs) {
      try {
        if (!row.get(pair.col).getStringValue().equals(pair.val)) {
          LOG.info(
              "Changelog table value for col "
                  + pair.col
                  + " is: "
                  + row.get(pair.col).getStringValue()
                  + ", which is different from spanner table's value: "
                  + pair.val);
          return false;
        }
      } catch (BigQueryException e) {
        LOG.info("Error when trying to read the value for column " + pair.col + ": " + e);
        return false;
      }
    }
    return true;
  }

  class Pair {
    String col;
    String val;

    Pair(String col, String val) {
      this.col = col;
      this.val = val;
    }
  }

  public static int nextValue() {
    return counter.getAndIncrement();
  }

  private static Mutation generateTableRow(String tableId, int key, List<Pair> pairs) {
    Mutation.WriteBuilder mutation = Mutation.newInsertBuilder(tableId);
    mutation.set("Id").to(key);
    for (Pair pair : pairs) {
      mutation.set(pair.col).to(pair.val);
    }
    return mutation.build();
  }

  @Test
  public void testSpannerChangeStreamsToBigQueryFloatColumns() throws IOException {
    String spannerTable = testName + RandomStringUtils.randomAlphanumeric(1, 5);
    String createTableStatement =
        String.format(
            "CREATE TABLE %s (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  Float32Col FLOAT32,\n"
                + "  Float64Col FLOAT64,\n"
                + ") PRIMARY KEY(Id)",
            spannerTable);

    String cdcTable = spannerTable + "_changelog";
    spannerResourceManager.executeDdlStatement(createTableStatement);
    String createChangeStreamStatement =
        String.format(
            "CREATE CHANGE STREAM %s_stream FOR %s OPTIONS (value_capture_type = 'NEW_ROW')",
            testName, spannerTable);
    spannerResourceManager.executeDdlStatement(createChangeStreamStatement);
    bigQueryResourceManager.createDataset(REGION);

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();

    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("spannerProjectId", PROJECT)
                    .addParameter("spannerInstanceId", spannerResourceManager.getInstanceId())
                    .addParameter("spannerDatabase", spannerResourceManager.getDatabaseId())
                    .addParameter(
                        "spannerMetadataInstanceId", spannerResourceManager.getInstanceId())
                    .addParameter("spannerMetadataDatabase", spannerResourceManager.getDatabaseId())
                    .addParameter("spannerChangeStreamName", testName + "_stream")
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("rpcPriority", "HIGH")
                    .addParameter("disableDlqRetries", "true")));

    assertThatPipeline(launchInfo).isRunning();

    int key = 1;
    float float32Val = 3.14f;
    double float64Val = 2.71;

    Mutation expectedData =
        Mutation.newInsertBuilder(spannerTable)
            .set("Id")
            .to(key)
            .set("Float32Col")
            .to(float32Val)
            .set("Float64Col")
            .to(float64Val)
            .build();

    spannerResourceManager.write(Collections.singletonList(expectedData));
    String query = queryCdcTable(cdcTable, key);
    waitForQueryToReturnRows(query, 1, true);

    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    assertEquals(1, tableResult.getTotalRows());

    for (FieldValueList row : tableResult.iterateAll()) {
      assertEquals(float32Val, (float) (row.get("Float32Col").getDoubleValue()), 1e-6f);
      assertEquals(float64Val, row.get("Float64Col").getDoubleValue(), 1e-15);
    }
  }

  private String queryCdcTable(String cdcTable, int key) {
    return "SELECT * FROM `"
        + bigQueryResourceManager.getDatasetId()
        + "."
        + cdcTable
        + "`"
        + String.format(" WHERE Id = %d", key);
  }

  @NotNull
  private Supplier<Boolean> dataShownUp(String query, int minRows) {
    return () -> {
      try {
        return bigQueryResourceManager.runQuery(query).getTotalRows() >= minRows;
      } catch (Exception e) {
        if (ExceptionUtils.containsMessage(e, "Not found: Table")) {
          return false;
        } else {
          throw e;
        }
      }
    };
  }

  private void waitForQueryToReturnRows(String query, int resultsRequired, boolean cancelOnceDone)
      throws IOException {
    Config config = createConfig(launchInfo);
    Result result =
        cancelOnceDone
            ? pipelineOperator()
                .waitForConditionAndCancel(config, dataShownUp(query, resultsRequired))
            : pipelineOperator().waitForCondition(config, dataShownUp(query, resultsRequired));
    assertThatResult(result).meetsConditions();
  }
}
