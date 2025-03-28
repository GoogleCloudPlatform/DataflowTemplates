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
import java.util.HashMap;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Ignore("SQL instance running inside github runner is crashing")
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLSourceDBToSpannerWideRowMaxSizeNonKeyColumnsIT extends SourceDbToSpannerITBase {

  private static PipelineLauncher.LaunchInfo jobInfo;
  public static MySQLResourceManager mySQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  private static final String WORKER_MACHINE_TYPE = "n1-highmem-96";
  private static final String MYSQL_DUMP_FILE_RESOURCE =
      "WideRow/MaxSizeNonKeyColumnIT/mysql-test.sql";
  private static final String SPANNER_SCHEMA_FILE_RESOURCE =
      "WideRow/MaxSizeNonKeyColumnIT/spanner-test.sql";

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
  public void testMaxColumnsPerTable() throws Exception {
    loadSQLFileResource(mySQLResourceManager, MYSQL_DUMP_FILE_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_SCHEMA_FILE_RESOURCE);

    // Launch the migration job
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            mySQLResourceManager,
            spannerResourceManager,
            new HashMap<String, String>() {
              {
                put("workerMachineType", WORKER_MACHINE_TYPE);
              }
            },
            null);

    // Wait for job completion
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    ImmutableList<Struct> wideRowData =
        spannerResourceManager.readTableRecords("MaxNonKeyTable", "id");
    SpannerAsserts.assertThatStructs(wideRowData).hasRows(1);
  }
}
