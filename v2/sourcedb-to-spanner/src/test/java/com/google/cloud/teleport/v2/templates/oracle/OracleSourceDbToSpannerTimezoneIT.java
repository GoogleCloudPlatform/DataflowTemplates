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

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.cloud.teleport.v2.templates.SourceDbToSpannerITBase;
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

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class OracleSourceDbToSpannerTimezoneIT extends SourceDbToSpannerITBase {
  private PipelineLauncher.LaunchInfo jobInfo;

  private OracleResourceManager oracleResourceManager;
  private SpannerResourceManager spannerResourceManager;

  private static final String ORACLE_DDL_RESOURCE =
      "oracle/OracleSourceDbToSpannerTimezoneIT/oracle-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleSourceDbToSpannerTimezoneIT/oracle-spanner-schema.sql";

  @Before
  public void setUp() {
    oracleResourceManager = setUpOracleResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, oracleResourceManager);
  }

  @Test
  public void testOracleTimestampMapping() throws Exception {
    loadSQLFileResource(oracleResourceManager, ORACLE_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            oracleResourceManager,
            spannerResourceManager,
            getExtraJobParameters(),
            null);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));

    assertTimestampAndDatetimeBackfillContents();
  }

  private Map<String, String> getExtraJobParameters() {
    Map<String, String> extraParams = new HashMap<>();
    extraParams.put("jdbcDriverJars", oracleDriverGCSPath());
    return extraParams;
  }

  private void assertTimestampAndDatetimeBackfillContents() {
    List<Map<String, Object>> expectedRows = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("id", 1);
    row.put("timestamp_column", "2024-02-02T00:00:00Z");
    row.put("datetime_column", "2024-02-02T10:00:00Z");
    expectedRows.add(row);

    row = new HashMap<>();
    row.put("id", 2);
    row.put("timestamp_column", "2024-02-02T10:00:00Z");
    row.put("datetime_column", "2024-02-02T20:00:00Z");
    expectedRows.add(row);

    row = new HashMap<>();
    row.put("id", 3);
    row.put("timestamp_column", "2024-02-02T20:00:00Z");
    row.put("datetime_column", "2024-02-03T06:00:00Z");
    expectedRows.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select id, timestamp_column, datetime_column from DateData"))
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedRows);
  }
}
