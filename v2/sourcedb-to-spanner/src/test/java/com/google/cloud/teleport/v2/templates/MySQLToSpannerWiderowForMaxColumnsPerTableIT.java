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

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
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

  private String getMySQLDDL() {
    StringBuilder mysqlColumns = new StringBuilder();
    for (int i = 0; i < NUM_COLUMNS; i++) {
      mysqlColumns.append("col").append(i).append(" INT");
      if (i < NUM_COLUMNS - 1) {
        mysqlColumns.append(", ");
      }
    }
    return String.format(
        "CREATE TABLE %s (id INT NOT NULL, %s, PRIMARY KEY (id));", TABLENAME, mysqlColumns);
  }

  private String getSpannerDDL() {
    StringBuilder spannerColumns = new StringBuilder();
    for (int i = 0; i < NUM_COLUMNS; i++) {
      spannerColumns.append("col").append(i).append(" INT64");
      if (i < NUM_COLUMNS - 1) {
        spannerColumns.append(", ");
      }
    }
    return String.format(
        "CREATE TABLE %s (id INT64 NOT NULL, %s) PRIMARY KEY (id);", TABLENAME, spannerColumns);
  }

  private String getMySQLInsertStatement() {
    StringBuilder columns = new StringBuilder();
    StringBuilder values = new StringBuilder();
    columns.append("id");
    values.append("1");
    for (int i = 0; i < NUM_COLUMNS; i++) {
      columns.append(", col").append(i);
      values.append(",").append(i);
    }
    return String.format("INSERT INTO %s (%s) VALUES (%s);", TABLENAME, columns, values);
  }

  @Test
  public void testMaxColumnsPerTable() throws Exception {
    increasePacketSize();
    loadSQLToJdbcResourceManager(mySQLResourceManager, getMySQLDDL());
    loadSQLToJdbcResourceManager(mySQLResourceManager, getMySQLInsertStatement());
    spannerResourceManager.executeDdlStatement(getSpannerDDL());

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

    // create a list of columns to verify the data in Spanner
    List<String> expectedColumns = new ArrayList<>();
    expectedColumns.add("id");
    for (int i = 0; i < NUM_COLUMNS; i++) {
      expectedColumns.add("col" + i);
    }

    ImmutableList<Struct> wideRowData =
        spannerResourceManager.readTableRecords(TABLENAME, expectedColumns);
    SpannerAsserts.assertThatStructs(wideRowData).hasRows(1);
  }
}
