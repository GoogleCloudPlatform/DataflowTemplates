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
import java.util.StringJoiner;
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

  private static final List<String> columns = new ArrayList<>();

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
    StringBuilder ddl = new StringBuilder();
    ddl.append("CREATE TABLE " + TABLENAME + " (");
    ddl.append("id INT NOT NULL,");
    columns.add("id");
    for (int i = 0; i < NUM_COLUMNS - 1; i++) {
      ddl.append("col" + i + " INT,");
      columns.add("col" + i);
    }
    // Last column without trailing comma
    ddl.append("col" + (NUM_COLUMNS - 1) + " INT");
    columns.add("col" + (NUM_COLUMNS - 1));
    ddl.append(" PRIMARY KEY (id));");
    return ddl.toString();
  }

  private String getSpannerDDL() {
    StringBuilder schema = new StringBuilder();
    schema.append("CREATE TABLE " + TABLENAME + " (");
    schema.append("id INT64 NOT NULL,");
    for (int i = 0; i < NUM_COLUMNS - 1; i++) {
      schema.append("col" + i + " INT64,");
    }
    // Last column without trailing comma
    schema.append("col" + (NUM_COLUMNS - 1) + " INT64");
    schema.append(") PRIMARY KEY (id);");
    return schema.toString();
  }

  private String getMySQLInsertStatement() {
    // Use StringJoiner for efficiency
    StringJoiner columnNames = new StringJoiner(", ");
    StringJoiner values = new StringJoiner(", ");

    // Add the ID column
    columnNames.add("id");
    values.add("1");

    // Add all the other columns, ensuring we match exactly what's in the DDL
    for (int i = 0; i < NUM_COLUMNS - 1; i++) {
      columnNames.add("col" + i);
      values.add(String.valueOf(i + 1));
    }

    return String.format("INSERT INTO %s (%s) VALUES (%s);", TABLENAME, columnNames, values);
  }

  @Test
  public void testMaxColumnsPerTable() throws Exception {
    increasePacketSize();
    String mysqlSchema = getMySQLDDL();
    loadSQLToJdbcResourceManager(mySQLResourceManager, mysqlSchema);
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
    ImmutableList<Struct> wideRowData = spannerResourceManager.readTableRecords(TABLENAME, columns);
    SpannerAsserts.assertThatStructs(wideRowData).hasRows(0);
  }
}
