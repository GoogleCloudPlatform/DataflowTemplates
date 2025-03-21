/*
 * Copyright (C) 2025 Google LLC
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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLToSpanner5000TablePerDBIT extends SourceDbToSpannerITBase {

  private static final Logger LOG = LoggerFactory.getLogger(MySQLToSpanner5000TablePerDBIT.class);
  private static final int NUM_TABLES = 5000;
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static MySQLResourceManager mySQLResourceManager;
  private static SpannerResourceManager spannerResourceManager;

  @Before
  public void setUp() {
    mySQLResourceManager = setUpMySQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  @Test
  public void testMySQLToSpannerMigration() throws Exception {
    IntStream.range(0, NUM_TABLES).forEach(this::createSchemas);

    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            mySQLResourceManager,
            spannerResourceManager,
            null,
            null);

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    IntStream.range(0, NUM_TABLES).forEach(this::verifySpannerData);
  }

  private void createSchemas(int tableNumber) {
    String tableName = "table_" + tableNumber;
    createMySQLSchema(mySQLResourceManager, tableName);
    createSpannerSchema(spannerResourceManager, tableName);
  }

  private void verifySpannerData(int tableNumber) {
    String tableName = "table_" + tableNumber;
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.readTableRecords(tableName, "id", "name"))
        .hasRecordsUnorderedCaseInsensitiveColumns(getMySQLData());
  }

  private JDBCResourceManager.JDBCSchema getMySQLSchema() {
    Map<String, String> columns =
        Map.of(
            "id", "INTEGER NOT NULL",
            "name", "VARCHAR(20)");
    return new JDBCResourceManager.JDBCSchema(columns, "id");
  }

  private List<Map<String, Object>> getMySQLData() {
    return Collections.singletonList(
        Map.of("id", 1, "name", RandomStringUtils.randomAlphabetic(10)));
  }

  private void createMySQLSchema(MySQLResourceManager mySQLResourceManager, String tableName) {
    mySQLResourceManager.createTable(tableName, getMySQLSchema());
    mySQLResourceManager.write(tableName, getMySQLData());
  }

  private void createSpannerSchema(
      SpannerResourceManager spannerResourceManager, String tableName) {
    String createTableStatement =
        String.format(
            "CREATE TABLE %s (id INT64 NOT NULL, name STRING(20)) PRIMARY KEY (id)", tableName);
    spannerResourceManager.executeDdlStatement(createTableStatement);
  }
}
