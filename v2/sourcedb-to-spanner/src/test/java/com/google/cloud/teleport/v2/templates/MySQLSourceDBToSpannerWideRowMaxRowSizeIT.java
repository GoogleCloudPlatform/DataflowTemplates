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
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
@Ignore("Causes OOMs with low-provisioned VMs/DBs")
public class MySQLSourceDBToSpannerWideRowMaxRowSizeIT extends SourceDbToSpannerITBase {

  private static boolean initialized = false;
  private static boolean dataLoaded = false;
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static MySQLResourceManager mySQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager pgDialectSpannerResourceManager;

  private static final String MYSQL_DUMP_FILE_RESOURCE = "WideRow/MaxRowSize/mysql-schema.sql";
  private static final String SPANNER_SCHEMA_FILE_RESOURCE =
      "WideRow/MaxRowSize/spanner-schema.sql";
  private static final String PG_DIALECT_SPANNER_SCHEMA_FILE_RESOURCE =
      "WideRow/MaxRowSize/pg-dialect-spanner-schema.sql";

  private static final String TABLE = "WideRowTable";

  @Before
  public void setUp() throws Exception {
    synchronized (MySQLSourceDBToSpannerWideRowMaxRowSizeIT.class) {
      if (!initialized) {
        mySQLResourceManager = setUpMySQLResourceManager();
        spannerResourceManager = setUpSpannerResourceManager();
        pgDialectSpannerResourceManager = setUpPGDialectSpannerResourceManager();
        initialized = true;
      }
      // Separate the initialization of the resources and the loading of data so that if the data
      // loading fails, the resources are not re-initialized (which would leave dangling resources
      // in GCP).
      if (!dataLoaded) {
        loadSQLFileResource(mySQLResourceManager, MYSQL_DUMP_FILE_RESOURCE);
        dataLoaded = true;
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    ResourceManagerUtils.cleanResources(
        mySQLResourceManager, spannerResourceManager, pgDialectSpannerResourceManager);
  }

  @Test
  public void wideRowMaxRowSize() throws Exception {
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

    // Verify the data in Spanner
    ImmutableList<Struct> wideRowData = spannerResourceManager.readTableRecords(TABLE, "id");
    SpannerAsserts.assertThatStructs(wideRowData).hasRows(1);
  }

  @Test
  public void wideRowMaxRowSizePGDialect() throws Exception {
    createSpannerDDL(pgDialectSpannerResourceManager, PG_DIALECT_SPANNER_SCHEMA_FILE_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            mySQLResourceManager,
            pgDialectSpannerResourceManager,
            null,
            null);

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    // Verify the data in Spanner
    ImmutableList<Struct> wideRowData =
        pgDialectSpannerResourceManager.readTableRecords(TABLE, "id");
    SpannerAsserts.assertThatStructs(wideRowData).hasRows(1);
  }
}
