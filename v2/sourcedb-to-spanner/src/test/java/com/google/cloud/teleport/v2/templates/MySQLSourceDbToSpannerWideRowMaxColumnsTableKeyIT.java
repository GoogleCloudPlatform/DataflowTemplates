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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)

// This test is constrained to 16 columns in the primary key for both Spanner and MySQL, with a
// MySQL size limit of 3072 bytes.
public class MySQLSourceDbToSpannerWideRowMaxColumnsTableKeyIT extends SourceDbToSpannerITBase {
  private static final String TABLE_NAME = "LargePrimaryKeyTable";
  private static final String MYSQL_DUMP_FILE_RESOURCE =
      "WideRow/MaxColumnsTableKeyIT/mysql-schema.sql";
  private static final String SPANNER_SCHEMA_FILE_RESOURCE =
      "WideRow/MaxColumnsTableKeyIT/spanner-schema.sql";

  private static PipelineLauncher.LaunchInfo jobInfo;
  public static MySQLResourceManager mySQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  @Before
  public void setUp() throws Exception {
    mySQLResourceManager = setUpMySQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() throws Exception {
    ResourceManagerUtils.cleanResources(mySQLResourceManager, spannerResourceManager);
  }

  @Test
  public void wideRowMaxColumnsTableKeyTest() throws Exception {
    loadSQLFileResource(mySQLResourceManager, MYSQL_DUMP_FILE_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_SCHEMA_FILE_RESOURCE);
    ADDITIONAL_JOB_PARAMS.putAll(
        new HashMap<>() {
          {
            put("network", VPC_NAME);
            put("subnetwork", SUBNET_NAME);
            put("workerRegion", VPC_REGION);
          }
        });
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

    List<String> columns = new ArrayList<>();
    for (int i = 1; i <= 16; i++) {
      columns.add("pk_col" + i);
    }

    ImmutableList<Struct> wideRowData =
        spannerResourceManager.readTableRecords(TABLE_NAME, columns);
    SpannerAsserts.assertThatStructs(wideRowData).hasRows(1);
  }
}
