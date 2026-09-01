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

import com.google.cloud.teleport.metadata.DirectRunnerTest;
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
import org.apache.beam.it.jdbc.MSSQLResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which migrates date/time columns
 * from SQL Server into a Spanner database whose default time zone is not UTC, and checks that no
 * values are shifted.
 */
@Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class SQLServerSourceDbToSpannerTimezoneIT extends SourceDbToSpannerITBase {
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static MSSQLResourceManager msSqlResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  private static final String SQLSERVER_DDL_RESOURCE = "TimezoneIT/sqlserver-schema.sql";
  private static final String SPANNER_DDL_RESOURCE = "TimezoneIT/spanner-schema.sql";

  /** Setup resource managers. */
  @Before
  public void setUp() {
    msSqlResourceManager = setUpMSSQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, msSqlResourceManager);
  }

  @Test
  public void testSQLServerDatetimeOffsetAndDatetime2Mapping() throws Exception {
    loadSQLFileResource(msSqlResourceManager, SQLSERVER_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            msSqlResourceManager,
            spannerResourceManager,
            null,
            null);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    assertTimestampAndDatetimeBackfillContents();
  }

  private void assertTimestampAndDatetimeBackfillContents() {
    List<Map<String, Object>> expectedRows = new ArrayList<>();

    // DATETIMEOFFSET values carry an explicit +10:00 offset and are normalised to UTC, while
    // DATETIME2 values have no zone and are read as UTC wall-clock time.
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
