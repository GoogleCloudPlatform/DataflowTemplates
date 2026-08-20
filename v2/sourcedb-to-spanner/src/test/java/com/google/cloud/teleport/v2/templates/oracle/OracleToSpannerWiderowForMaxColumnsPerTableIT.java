/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.oracle;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.cloud.teleport.v2.templates.SourceDbToSpannerITBase;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.OracleResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for testing Oracle to Spanner migration with wide tables (1000 columns). */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class OracleToSpannerWiderowForMaxColumnsPerTableIT extends SourceDbToSpannerITBase {
  private PipelineLauncher.LaunchInfo jobInfo;
  private OracleResourceManager oracleResourceManager;
  private SpannerResourceManager spannerResourceManager;

  private static final String TABLENAME = "WiderowTable";

  @Before
  public void setUp() {
    oracleResourceManager = setUpOracleResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, oracleResourceManager);
  }

  private String getOracleInsertStatement(int maxColumns) {
    StringBuilder columns = new StringBuilder();
    StringBuilder values = new StringBuilder();

    columns.append("\"id\",");
    values.append("1, ");

    for (int i = 1; i <= maxColumns; i++) {
      columns.append("\"col" + i + "\"");
      values.append(i);
      if (i != maxColumns) {
        columns.append(", ");
        values.append(", ");
      }
    }

    return String.format("INSERT INTO \"%s\" (%s) VALUES (%s)", TABLENAME, columns, values);
  }

  private List<String> getColumnsList(int maxColumns) {
    List<String> columns = new ArrayList<>();
    columns.add("id");
    for (int i = 1; i <= maxColumns; i++) {
      columns.add("col" + i);
    }
    return columns;
  }

  @Test
  public void testMaxColumnsPerTable() throws Exception {
    int maxColumns = 999;

    loadOracleSQLFileResource(
        oracleResourceManager,
        "oracle/OracleToSpannerWiderowForMaxColumnsPerTableIT/oracle-schema.sql");
    loadOracleSQLToJdbcResourceManager(oracleResourceManager, getOracleInsertStatement(maxColumns));

    createSpannerDDL(
        spannerResourceManager,
        "oracle/OracleToSpannerWiderowForMaxColumnsPerTableIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql");

    Map<String, String> jobParams = new HashMap<>();
    jobParams.put("jdbcDriverJars", oracleDriverGCSPath());

    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            oracleResourceManager,
            spannerResourceManager,
            jobParams,
            null);

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    List<String> expectedColumns = getColumnsList(maxColumns);
    ImmutableList<Struct> wideRowData =
        spannerResourceManager.readTableRecords(TABLENAME, expectedColumns);

    SpannerAsserts.assertThatStructs(wideRowData).hasRows(1);
  }
}
