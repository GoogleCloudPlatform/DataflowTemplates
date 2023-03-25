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
package com.google.cloud.teleport.templates;

import static com.google.cloud.teleport.it.TestProperties.getProperty;
import static com.google.cloud.teleport.it.matchers.RecordsSubject.bigQueryRowsToRecords;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatDatastoreRecords;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.Tuple;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.datastore.Entity;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.BigQueryTestUtils;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.cloud.teleport.it.datastore.DatastoreResourceManager;
import com.google.cloud.teleport.it.datastore.DefaultDatastoreResourceManager;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link BigQueryToDatastore}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigQueryToDatastore.class)
@RunWith(JUnit4.class)
public final class BigQueryToDatastoreIT extends TemplateTestBase {

  private BigQueryResourceManager bigQueryResourceManager;
  private DatastoreResourceManager datastoreResourceManager;

  // Define a set of parameters used to allow configuration of the test size being run.
  private static final String BIGQUERY_ID_COL = "test_id";
  private static final int BIGQUERY_NUM_ROWS =
      Integer.parseInt(getProperty("numRows", "100", TestProperties.Type.PROPERTY));
  private static final int BIGQUERY_NUM_FIELDS =
      Integer.parseInt(getProperty("numFields", "20", TestProperties.Type.PROPERTY));
  private static final int BIGQUERY_MAX_ENTRY_LENGTH =
      Integer.min(
          300, Integer.parseInt(getProperty("maxEntryLength", "20", TestProperties.Type.PROPERTY)));

  @Before
  public void setup() {
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testName, PROJECT)
            .setCredentials(credentials)
            .build();
    datastoreResourceManager =
        DefaultDatastoreResourceManager.builder(testId).credentials(credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryResourceManager, datastoreResourceManager);
  }

  @Test
  public void testBigQueryToDatastore() throws IOException {
    // Arrange
    Tuple<Schema, List<RowToInsert>> generatedTable =
        BigQueryTestUtils.generateBigQueryTable(
            BIGQUERY_ID_COL, BIGQUERY_NUM_ROWS, BIGQUERY_NUM_FIELDS, BIGQUERY_MAX_ENTRY_LENGTH);
    Schema bigQuerySchema = generatedTable.x();
    List<RowToInsert> bigQueryRows = generatedTable.y();
    TableId table = bigQueryResourceManager.createTable(testName, bigQuerySchema);
    bigQueryResourceManager.write(testName, bigQueryRows);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("readQuery", "SELECT * FROM `" + toTableSpecStandard(table) + "`")
            .addParameter("readIdColumn", BIGQUERY_ID_COL)
            .addParameter("datastoreWriteProjectId", PROJECT)
            .addParameter("datastoreWriteEntityKind", "person")
            .addParameter("datastoreWriteNamespace", testId)
            .addParameter("datastoreHintNumWorkers", "1")
            .addParameter("errorWritePath", getGcsPath("errorWritePath"))
            .addParameter("invalidOutputPath", getGcsPath("invalidOutputPath"));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Entity> queryResults = datastoreResourceManager.query("SELECT * from person");
    assertThat(queryResults).isNotEmpty();

    assertThatDatastoreRecords(queryResults)
        .hasRecordsUnordered(
            bigQueryRowsToRecords(bigQueryRows, ImmutableList.of(BIGQUERY_ID_COL)));
  }
}
