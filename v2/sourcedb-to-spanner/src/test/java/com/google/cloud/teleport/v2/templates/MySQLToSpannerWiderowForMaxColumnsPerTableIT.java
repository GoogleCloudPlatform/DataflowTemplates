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
import java.io.IOException;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLToSpannerWiderowForMaxColumnsPerTableIT extends SourceDbToSpannerITBase {
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static final Integer NUM_COLUMNS = 1023;
  private static final String TABLENAME = "WiderowTable";
  private static final int MAX_ALLOWED_PACKET = 128 * 1024 * 1024; // 128 MiB

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

  private void increasePacketSize() {
    String allowedGlobalPacket = "SET GLOBAL max_allowed_packet = " + MAX_ALLOWED_PACKET;
    mySQLResourceManager.runSQLUpdate(allowedGlobalPacket);
  }

  private JDBCResourceManager.JDBCSchema getMySQLSchema() {
    HashMap<String, String> columns = new HashMap<>();
    columns.put("id", "INT");
    for (int i = 0; i < NUM_COLUMNS; i++) {
      columns.put("col" + i, "INT");
    }
    return new JDBCResourceManager.JDBCSchema(columns, "id");
  }

  private String getSpannerSchema() {
    StringBuilder schema = new StringBuilder();
    schema.append("CREATE TABLE WiderowTable (");
    schema.append("id INT64 NOT NULL,");
    for (int i = 0; i < NUM_COLUMNS; i++) {
      schema.append("col" + i + " INT64,");
    }
    schema.append(") PRIMARY KEY (id)");
    return schema.toString();
  }

  private List<Map<String, Object>> getMySQLData() {
    List<Map<String, Object>> data = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", i);
      for (int j = 0; j < NUM_COLUMNS; j++) {
        row.put("col" + j, i + j);
      }
      data.add(row);
    }
    return data;
  }

  @Test
  public void testMaxColumnsPerTable() throws IOException {
    increasePacketSize();
    mySQLResourceManager.createTable(TABLENAME, getMySQLSchema());
    mySQLResourceManager.write(TABLENAME, getMySQLData());
    createSpannerDDL(spannerResourceManager, getSpannerSchema());
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
    List<String> columns = new ArrayList<>();
    columns.add("id");
    for (int i = 1; i <= NUM_COLUMNS; i++) {
      columns.add("col" + i);
    }
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.readTableRecords("WiderowTable", columns))
        .hasRecordsUnorderedCaseInsensitiveColumns(getMySQLData());
  }
}
