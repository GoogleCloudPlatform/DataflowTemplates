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
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.OracleResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class OracleSourceDBToSpannerWideRowMaxSizeStringIT extends SourceDbToSpannerITBase {

  private static boolean initialized = false;
  private PipelineLauncher.LaunchInfo jobInfo;
  private static OracleResourceManager oracleResourceManager;
  private static SpannerResourceManager spannerResourceManager;

  private static final String ORACLE_DUMP_FILE_RESOURCE =
      "oracle/OracleSourceDBToSpannerWideRowMaxSizeStringIT/oracle-schema.sql";
  private static final String SPANNER_SCHEMA_FILE_RESOURCE =
      "oracle/OracleSourceDBToSpannerWideRowMaxSizeStringIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";

  private static final String TABLE = "WideRowTable";

  /** Setup resource managers once during the execution of this test class. */
  @Before
  public void setUp() throws Exception {
    synchronized (OracleSourceDBToSpannerWideRowMaxSizeStringIT.class) {
      if (!initialized) {
        oracleResourceManager = SharedOracleBulkITContainer.getInstance();
        spannerResourceManager = setUpSpannerResourceManager();
        testUsername = setupOracleIsolatedUser(oracleResourceManager);

        loadOracleSQLFileResource(oracleResourceManager, ORACLE_DUMP_FILE_RESOURCE, testUsername);

        initialized = true;
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void wideRowMaxSizeString() throws Exception {
    createSpannerDDL(spannerResourceManager, SPANNER_SCHEMA_FILE_RESOURCE);
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

    // Verify the data in Spanner
    ImmutableList<Struct> wideRowData =
        spannerResourceManager.readTableRecords(
            TABLE, "id", "max_string_col_to_bytes", "max_string_col_to_str");
    SpannerAsserts.assertThatStructs(wideRowData).hasRows(1);
  }
}
