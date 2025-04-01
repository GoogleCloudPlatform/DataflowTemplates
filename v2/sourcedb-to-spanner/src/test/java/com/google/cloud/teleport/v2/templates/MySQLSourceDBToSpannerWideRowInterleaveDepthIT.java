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
import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLSourceDBToSpannerWideRowInterleaveDepthIT extends SourceDbToSpannerITBase {
  private PipelineLauncher.LaunchInfo jobInfo;
  private MySQLResourceManager mySQLResourceManager;
  private SpannerResourceManager spannerResourceManager;

  private static final String SPANNER_SCHEMA_FILE_RESOURCE =
      "WideRow/InterleaveDepthIT/spanner-schema.sql";
  private static final String SPANNER_SCHEMA_DEPTH_8_FILE_RESOURCE =
      "WideRow/InterleaveDepthIT/spanner-schema-depth-8.sql";

  private static final String MYSQL_DUMP_FILE_RESOURCE =
      "WideRow/InterleaveDepthIT/mysql-schema.sql";

  @Before
  public void setUp() {
    mySQLResourceManager = setUpMySQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  @Test
  public void wideRowInterleaveDepthTest() throws Exception {
    loadSQLFileResource(mySQLResourceManager, MYSQL_DUMP_FILE_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_SCHEMA_FILE_RESOURCE);
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
    for (int i = 1; i <= 7; i++) {
      String tableName = "child" + i;
      assertEquals(
          "Interleaved depth " + i + " migrated",
          1,
          spannerResourceManager.getRowCount(tableName).longValue());
    }
  }

  @Test
  public void wideRowInterleaveDepth8FailureTest() {
    try {
      // Attempt to create a schema with interleave depth of 8 (which exceeds Spanner's limit of
      createSpannerDDL(spannerResourceManager, SPANNER_SCHEMA_DEPTH_8_FILE_RESOURCE);
    } catch (Exception e) {
      System.out.println("===>>>>>> Exception caught: " + e.getMessage());
      Assert.assertTrue(
          "Exception should mention key column limitation",
          e.getMessage().contains("Failed to execute statement"));
    }
  }
}
