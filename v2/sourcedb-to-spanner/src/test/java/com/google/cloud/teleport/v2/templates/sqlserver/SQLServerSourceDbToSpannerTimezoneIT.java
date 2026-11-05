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
package com.google.cloud.teleport.v2.templates.sqlserver;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.DirectRunnerTest;
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
 *
 * <p>SQL Server has no session time zone (unlike MySQL's {@code SET time_zone} or Oracle's {@code
 * ALTER SESSION SET TIME_ZONE}), so the source-side conversion is exercised by having the engine
 * apply a named non-UTC zone to naive literals via {@code AT TIME ZONE}. The remaining columns
 * cover naive {@code DATETIME2}, explicit-offset {@code DATETIMEOFFSET} and legacy {@code
 * DATETIME}.
 */
@Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class SQLServerSourceDbToSpannerTimezoneIT extends SourceDbToSpannerITBase {
  public MSSQLResourceManager msSqlResourceManager;
  public SpannerResourceManager spannerResourceManager;

  private static final String SQLSERVER_DDL_RESOURCE =
      "sqlserver/SQLServerSourceDbToSpannerTimezoneIT/sqlserver-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/SQLServerSourceDbToSpannerTimezoneIT/sqlserver-spanner-schema.sql";

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
  public void testSQLServerTimezoneMapping() throws Exception {
    loadSQLFileResource(msSqlResourceManager, SQLSERVER_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    PipelineLauncher.LaunchInfo jobInfo =
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

    // timestamp_column: naive literal converted by SQL Server itself with
    // AT TIME ZONE 'E. Australia Standard Time' (+10:00) -> must land as the UTC instant.
    // timestamp_tz_column: same instant written with an explicit +10:00 offset.
    // datetime_column (DATETIME2) / legacy_datetime_column (DATETIME): zone-less, so the wall-clock
    // value must arrive as-is in UTC, unshifted by either the source zone or Spanner's
    // Australia/Brisbane default_time_zone.
    expectedRows.add(expectedRow(1, "2024-02-02T00:00:00Z", "2024-02-02T10:00:00Z"));
    expectedRows.add(expectedRow(2, "2024-02-02T10:00:00Z", "2024-02-02T20:00:00Z"));
    expectedRows.add(expectedRow(3, "2024-02-02T20:00:00Z", "2024-02-03T06:00:00Z"));

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select id, timestamp_column, datetime_column, timestamp_tz_column,"
                    + " legacy_datetime_column from DateData"))
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedRows);
  }

  private static Map<String, Object> expectedRow(int id, String utcInstant, String wallClock) {
    Map<String, Object> row = new HashMap<>();
    row.put("id", id);
    row.put("timestamp_column", utcInstant);
    row.put("timestamp_tz_column", utcInstant);
    row.put("datetime_column", wallClock);
    row.put("legacy_datetime_column", wallClock);
    return row;
  }
}
