/*
 * Copyright (C) 2024 Google LLC
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
import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.cloud.teleport.v2.templates.SourceDbToSpannerITBase;
import java.util.Arrays;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests FK dependency
 * migration.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class OracleForeignKeyDependencyIT extends SourceDbToSpannerITBase {
  private static final Logger LOG = LoggerFactory.getLogger(OracleForeignKeyDependencyIT.class);
  private PipelineLauncher.LaunchInfo jobInfo;

  private org.apache.beam.it.jdbc.JDBCResourceManager oracleResourceManager;
  private SpannerResourceManager spannerResourceManager;

  private static final String ORACLE_DUMP_FILE_RESOURCE =
      "oracle/OracleForeignKeyDependencyIT/oracle-schema.sql";

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleForeignKeyDependencyIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class. \
   */
  @Before
  public void setUp() {
    oracleResourceManager = SharedOracleBulkITContainer.getInstance();
    spannerResourceManager = setUpSpannerResourceManager();
    testUsername = setupOracleIsolatedUser(oracleResourceManager);
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void linearDependencyTest() throws Exception {
    loadOracleSQLFileResource(oracleResourceManager, ORACLE_DUMP_FILE_RESOURCE, testUsername);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    java.util.Map<String, String> jobParams = new java.util.HashMap<>();
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

    for (String tableName :
        Arrays.asList("t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10")) {
      assertEquals(
          "Asserting count for " + tableName,
          10L,
          spannerResourceManager.getRowCount(tableName).longValue());
    }
  }
}
