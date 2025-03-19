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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLToSpanner5000TablePerDBIT extends SourceDbToSpannerITBase {
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static MySQLResourceManager mySQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  private static final String SPANNER_DDL_RESOURCE = "";
  private static final Integer NUM_TABLES = 5000;

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
  public void testMySQLToSpanner5000TablePerDB() throws Exception {
    for (int i = 0; i < NUM_TABLES; i++) {
      String tableName = "table_" + i;
      createMysqlSchema(mySQLResourceManager, tableName);
      createSpannerSchema(spannerResourceManager, tableName);
    }
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

    for (int i = 0; i < NUM_TABLES; i++) {
      SpannerAsserts.assertThatStructs(
              spannerResourceManager.readTableRecords("table_" + i, "id", "name"))
          .hasRecordsUnorderedCaseInsensitiveColumns(getMySQLData());
    }
  }

  private JDBCResourceManager.JDBCSchema getMySQLSchema() {
    HashMap<String, String> columns = new HashMap<>();
    columns.put("id", "INTEGER NOT NULL");
    columns.put("name", "VARCHAR(200)");
    return new JDBCResourceManager.JDBCSchema(columns, "id");
  }

  private List<Map<String, Object>> getMySQLData() {
    List<Map<String, Object>> data = new ArrayList<>();
    Map<String, Object> values = new HashMap<>();
    values.put("id", 1);
    values.put("name", RandomStringUtils.randomAlphabetic(10));
    data.add(values);
    return data;
  }

  private void createMysqlSchema(MySQLResourceManager mySQLResourceManager, String tableName) {
    mySQLResourceManager.createTable(tableName, getMySQLSchema());
    mySQLResourceManager.write(tableName, getMySQLData());
  }

  private void createSpannerSchema(
      SpannerResourceManager spannerResourceManager, String tableName) {
    spannerResourceManager.executeDdlStatement(
        String.format(
            "CREATE TABLE %s (id INT64 NOT NULL, name STRING(200)) PRIMARY KEY (id)", tableName));
  }
}
