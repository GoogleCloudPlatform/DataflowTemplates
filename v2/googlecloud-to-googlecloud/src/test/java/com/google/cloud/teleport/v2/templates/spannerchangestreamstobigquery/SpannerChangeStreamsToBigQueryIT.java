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
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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
import org.junit.Ignore;
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
@Category(TemplateIntegrationTest.class)
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

    int key = nextValue();
    String firstName = UUID.randomUUID().toString();
    String lastName = UUID.randomUUID().toString();
    Mutation insertOneRow =
        Mutation.newInsertBuilder(spannerTable)
            .set("Id")
            .to(key)
            .set("FirstName")
            .to(firstName)
            .set("LastName")
            .to(lastName)
            .build();
    spannerResourceManager.write(Collections.singletonList(insertOneRow));

    String createChangeStreamStatement =
        String.format("CREATE CHANGE STREAM %s_stream FOR %s", testName, spannerTable);
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
                    .addParameter("dlqRetryMinutes", "3")));

    assertThatPipeline(launchInfo).isRunning();

    String updatedLastName = UUID.randomUUID().toString();
    Mutation updateOneRow =
        Mutation.newUpdateBuilder(spannerTable)
            .set("Id")
            .to(key)
            .set("LastName")
            .to(updatedLastName)
            .build();
    spannerResourceManager.write(Collections.singletonList(updateOneRow));
    String query = queryCdcTable(cdcTable, key);
    waitForQueryToReturnRows(query, 1, true);

    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    assertEquals(1, tableResult.getTotalRows());
    for (FieldValueList row : tableResult.iterateAll()) {
      assertEquals(firstName, row.get("FirstName").getStringValue());
      assertEquals(updatedLastName, row.get("LastName").getStringValue());
    }
  }

  @Test
  public void testSpannerChangeStreamsToBigQueryBasicWriteApiExactlyOnce() throws IOException {
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

    int key = nextValue();
    String firstName = UUID.randomUUID().toString();
    String lastName = UUID.randomUUID().toString();
    Mutation insertOneRow =
        Mutation.newInsertBuilder(spannerTable)
            .set("Id")
            .to(key)
            .set("FirstName")
            .to(firstName)
            .set("LastName")
            .to(lastName)
            .build();
    spannerResourceManager.write(Collections.singletonList(insertOneRow));

    String createChangeStreamStatement =
        String.format("CREATE CHANGE STREAM %s_stream FOR %s", testName, spannerTable);
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
                    .addParameter("useStorageWriteApi", "true")
                    .addParameter("useStorageWriteApiAtLeastOnce", "false")
                    .addParameter("numStorageWriteApiStreams", "1")
                    .addParameter("storageWriteApiTriggeringFrequencySec", "10")
                    .addParameter("dlqRetryMinutes", "3")));

    assertThatPipeline(launchInfo).isRunning();

    String updatedLastName = UUID.randomUUID().toString();
    Mutation updateOneRow =
        Mutation.newUpdateBuilder(spannerTable)
            .set("Id")
            .to(key)
            .set("LastName")
            .to(updatedLastName)
            .build();
    spannerResourceManager.write(Collections.singletonList(updateOneRow));
    String query = queryCdcTable(cdcTable, key);
    waitForQueryToReturnRows(query, 1, true);

    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    assertEquals(1, tableResult.getTotalRows());
    for (FieldValueList row : tableResult.iterateAll()) {
      assertEquals(firstName, row.get("FirstName").getStringValue());
      assertEquals(updatedLastName, row.get("LastName").getStringValue());
    }
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
                    .addParameter("dlqRetryMinutes", "3")));

    assertThatPipeline(launchInfo).isRunning();

    int key = nextValue();
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

  @Test
  public void testSpannerChangeStreamsToBigQueryAddTable() throws Exception {
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
            "CREATE CHANGE STREAM %s_stream FOR ALL OPTIONS (value_capture_type = 'NEW_VALUES')",
            testName);
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
                    .addParameter("dlqRetryMinutes", "3")));

    assertThatPipeline(launchInfo).isRunning();

    int key = nextValue();
    String firstName = UUID.randomUUID().toString();
    Mutation insertOneRow =
        Mutation.newInsertBuilder(spannerTable)
            .set("Id")
            .to(key)
            .set("FirstName")
            .to(firstName)
            .build();
    spannerResourceManager.write(Collections.singletonList(insertOneRow));
    String query = queryCdcTable(cdcTable, key);
    waitForQueryToReturnRows(query, 1, false);
    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    assertEquals(1, tableResult.getTotalRows());
    for (FieldValueList row : tableResult.iterateAll()) {
      assertEquals(firstName, row.get("FirstName").getStringValue());
      assertTrue(row.get("LastName").isNull());
    }

    String spannerTable2 = spannerTable + "_new";
    String cdcTable2 = spannerTable2 + "_changelog";
    String createTableStatement2 =
        String.format(
            "CREATE TABLE %s (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  FirstName String(1024),\n"
                + "  LastName String(1024),\n"
                + ") PRIMARY KEY(Id)",
            spannerTable2);
    spannerResourceManager.executeDdlStatement(createTableStatement2);

    int key2 = nextValue();
    String lastName2 = UUID.randomUUID().toString();
    Mutation insertOneRow2 =
        Mutation.newInsertBuilder(spannerTable2)
            .set("Id")
            .to(key2)
            .set("LastName")
            .to(lastName2)
            .build();
    spannerResourceManager.write(Collections.singletonList(insertOneRow2));

    String query2 = queryCdcTable(cdcTable2, key2);
    waitForQueryToReturnRows(query2, 1, false);
    TableResult tableResult2 = bigQueryResourceManager.runQuery(query2);
    assertEquals(1, tableResult2.getTotalRows());
    for (FieldValueList row : tableResult2.iterateAll()) {
      assertTrue(row.get("FirstName").isNull());
      assertEquals(lastName2, row.get("LastName").getStringValue());
    }
  }

  @Test
  @Ignore("This test requires sleep for >10 mins due to limitation of big query")
  public void testSpannerChangeStreamsToBigQueryAddColumn() throws Exception {
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

    int key = nextValue();
    String firstName = UUID.randomUUID().toString();
    String lastName = UUID.randomUUID().toString();
    Mutation insertOneRow =
        Mutation.newInsertBuilder(spannerTable)
            .set("Id")
            .to(key)
            .set("FirstName")
            .to(firstName)
            .set("LastName")
            .to(lastName)
            .build();
    spannerResourceManager.write(Collections.singletonList(insertOneRow));

    String createChangeStreamStatement =
        String.format("CREATE CHANGE STREAM %s_stream FOR %s", testName, spannerTable);
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
                    .addParameter("dlqRetryMinutes", "3")));

    assertThatPipeline(launchInfo).isRunning();

    Mutation deleteOneRow = Mutation.delete(spannerTable, Key.of(key));
    spannerResourceManager.write(Collections.singletonList(deleteOneRow));
    String query = queryCdcTable(cdcTable, key);
    waitForQueryToReturnRows(query, 1, false);

    String alterTableStatement =
        String.format("ALTER TABLE %s ADD Password STRING(MAX)", spannerTable);
    spannerResourceManager.executeDdlStatement(alterTableStatement);
    addEmptyColumn("Password", cdcTable);

    // Wait for bq streaming table metadata to notice the updated schema
    TimeUnit.MINUTES.sleep(15);

    int key2 = nextValue();
    String firstName2 = UUID.randomUUID().toString();
    String lastName2 = UUID.randomUUID().toString();
    String passWord = UUID.randomUUID().toString();
    Mutation insertOneRow2 =
        Mutation.newInsertBuilder(spannerTable)
            .set("Id")
            .to(key2)
            .set("FirstName")
            .to(firstName2)
            .set("LastName")
            .to(lastName2)
            .set("Password")
            .to(passWord)
            .build();
    spannerResourceManager.write(Collections.singletonList(insertOneRow2));

    String query2 = queryCdcTable(cdcTable, key2);
    waitForQueryToReturnRows(query2, 1, true);
    TableResult tableResult2 = bigQueryResourceManager.runQuery(query2);
    assertEquals(1, tableResult2.getTotalRows());
    for (FieldValueList row : tableResult2.iterateAll()) {
      assertEquals(firstName2, row.get("FirstName").getStringValue());
      assertEquals(lastName2, row.get("LastName").getStringValue());
      assertEquals(passWord, row.get("Password").getStringValue());
    }

    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    assertEquals(1, tableResult.getTotalRows());
    // Verify that the new column "Password" for older row is populated with null
    for (FieldValueList row : tableResult.iterateAll()) {
      assertTrue(row.get("FirstName").isNull());
      assertTrue(row.get("LastName").isNull());
      assertTrue(row.get("Password").isNull());
    }
  }

  public static int nextValue() {
    return counter.getAndIncrement();
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

  public void addEmptyColumn(String newColumnName, String tableId) {
    try {

      Table table = bigQueryResourceManager.getTableIfExists(tableId);
      Schema schema = table.getDefinition().getSchema();
      FieldList fields = schema.getFields();

      // Create the new field/column
      Field newField = Field.of(newColumnName, LegacySQLTypeName.STRING);

      // Create a new schema adding the current fields, plus the new one
      List<Field> fieldList = new ArrayList<Field>();
      fields.forEach(fieldList::add);
      fieldList.add(newField);
      Schema newSchema = Schema.of(fieldList);

      // Update the table with the new schema
      Table updatedTable =
          table.toBuilder().setDefinition(StandardTableDefinition.of(newSchema)).build();
      updatedTable.update();
    } catch (BigQueryException e) {
      LOG.info(
          "Caught exception when trying to add a new column to bigquery changelog table. \n"
              + e.toString());
    }
  }
}
