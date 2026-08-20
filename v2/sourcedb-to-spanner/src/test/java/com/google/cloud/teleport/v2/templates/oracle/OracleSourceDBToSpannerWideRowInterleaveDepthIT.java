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
import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.cloud.teleport.v2.templates.SourceDbToSpannerITBase;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
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
public class OracleSourceDBToSpannerWideRowInterleaveDepthIT extends SourceDbToSpannerITBase {
  private PipelineLauncher.LaunchInfo jobInfo;
  private OracleResourceManager oracleResourceManager;
  private SpannerResourceManager spannerResourceManager;

  private static final String SPANNER_SCHEMA_FILE_RESOURCE =
      "oracle/OracleSourceDBToSpannerWideRowInterleaveDepthIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";

  private static final String ORACLE_DUMP_FILE_RESOURCE =
      "oracle/OracleSourceDBToSpannerWideRowInterleaveDepthIT/oracle-schema.sql";

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
  public void wideRowInterleaveDepthTest() throws Exception {
    loadOracleSQLFileResource(oracleResourceManager, ORACLE_DUMP_FILE_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_SCHEMA_FILE_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            oracleResourceManager,
            spannerResourceManager,
            null,
            null);

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();
    for (int i = 1; i <= 7; i++) {
      // We use the case-preserved unquoted names in Spanner by default if they were created this
      // way.
      // The original MySQL test used: String tableName = "Level" + i;
      // In Spanner they should just be "Level" + i because GSQL is case-independent on querying but
      // stores as "Level..."
      String tableName = "Level" + i;
      assertEquals(
          "Interleaved depth " + i + " migrated",
          1,
          spannerResourceManager.getRowCount(tableName).longValue());
    }
  }
}
