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

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.cloud.teleport.v2.templates.SourceDbToSpannerITBase;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests a single sharded
 * migration on a simple schema from Oracle.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class OracleSingleShardIT extends SourceDbToSpannerITBase {
  private PipelineLauncher.LaunchInfo jobInfo;

  private org.apache.beam.it.jdbc.JDBCResourceManager oracleResourceManager;
  private SpannerResourceManager spannerResourceManager;

  private static final String ORACLE_DUMP_FILE_RESOURCE =
      "oracle/OracleSingleShardIT/oracle-schema.sql";

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleSingleShardIT/oracle-google_standard_sql-spanner-schema.sql";

  private static final String SESSION_FILE_RESOURCE = "oracle/OracleSingleShardIT/session.json";

  private static final String TABLE = "SingleShardWithTransformationTable";

  private static final String PKID = "pkid";

  private static final String NAME = "name";

  private static final String STATUS = "status";

  private static final String SHARD_ID = "migration_shard_id";

  @Before
  public void setUp() {
    oracleResourceManager = SharedOracleBulkITContainer.getInstance();
    spannerResourceManager = setUpSpannerResourceManager();
    testUsername = setupOracleIsolatedUser(oracleResourceManager);
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void singleShardWithIdPopulationTest() throws Exception {
    loadSQLFileResource(oracleResourceManager, ORACLE_DUMP_FILE_RESOURCE, testUsername);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    Map<String, String> jobParams = new HashMap<>();
    jobParams.put("jdbcDriverJars", oracleDriverGCSPath());

    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            SESSION_FILE_RESOURCE,
            null,
            oracleResourceManager,
            spannerResourceManager,
            jobParams,
            null);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.readTableRecords(TABLE, PKID, NAME, STATUS, SHARD_ID))
        .hasRecordsUnorderedCaseInsensitiveColumns(getExpectedData());
  }

  private List<Map<String, Object>> getExpectedData() {
    return List.of(
        Map.of(PKID, 1L, NAME, "Alice", STATUS, "active", SHARD_ID, "Shard1"),
        Map.of(PKID, 2L, NAME, "Bob", STATUS, "inactive", SHARD_ID, "Shard1"),
        Map.of(PKID, 3L, NAME, "Carol", STATUS, "pending", SHARD_ID, "Shard1"),
        Map.of(PKID, 4L, NAME, "David", STATUS, "complete", SHARD_ID, "Shard1"),
        Map.of(PKID, 5L, NAME, "Emily", STATUS, "error", SHARD_ID, "Shard1"));
  }
}
