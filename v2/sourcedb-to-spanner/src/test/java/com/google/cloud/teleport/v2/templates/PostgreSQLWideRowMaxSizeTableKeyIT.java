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
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.Ignore;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
@Ignore("Temporarily disabled for maintenance")
public class PostgreSQLWideRowMaxSizeTableKeyIT extends SourceDbToSpannerITBase {

  private static final String LARGE_PK_TABLE_NAME = "large_pk_table";
  private static final String LARGE_IDX_TABLE_NAME = "large_idx_table";
  private static final String POSTGRESQL_DUMP_FILE_RESOURCE =
      "WideRow/PostgreSQLMaxSizeTableKey/postgresql-schema.sql";
  private static final String SPANNER_SCHEMA_FILE_RESOURCE =
      "WideRow/PostgreSQLMaxSizeTableKey/spanner-schema.sql";
  private static final String PG_DIALECT_SPANNER_SCHEMA_FILE_RESOURCE =
      "WideRow/PostgreSQLMaxSizeTableKey/pg-dialect-spanner-schema.sql";

  private static boolean initialized = false;
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static PostgresResourceManager postgresResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager pgDialectSpannerResourceManager;

  @Before
  public void setUp() throws Exception {
    synchronized (PostgreSQLWideRowMaxSizePerCellIT.class) {
      if (!initialized) {
        postgresResourceManager = setUpPostgreSQLResourceManager();
        spannerResourceManager = setUpSpannerResourceManager();
        pgDialectSpannerResourceManager = setUpPGDialectSpannerResourceManager();

        loadSQLFileResource(postgresResourceManager, POSTGRESQL_DUMP_FILE_RESOURCE);

        initialized = true;
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    ResourceManagerUtils.cleanResources(
        postgresResourceManager, spannerResourceManager, pgDialectSpannerResourceManager);
  }

  @Test
  public void wideRowMaxSizeTableKey() throws Exception {
    createSpannerDDL(spannerResourceManager, SPANNER_SCHEMA_FILE_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            postgresResourceManager,
            spannerResourceManager,
            null,
            null);

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    // Verify the data in Spanner
    verifyData(spannerResourceManager);
  }

  @Test
  public void wideRowMaxSizeTableKeyPGDialect() throws Exception {
    createSpannerDDL(pgDialectSpannerResourceManager, PG_DIALECT_SPANNER_SCHEMA_FILE_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            postgresResourceManager,
            pgDialectSpannerResourceManager,
            null,
            null);

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    // Verify the data in Spanner
    verifyData(pgDialectSpannerResourceManager);
  }

  private void verifyData(SpannerResourceManager resourceManager) {
    ImmutableList<Struct> largePkData =
        resourceManager.readTableRecords(
            LARGE_PK_TABLE_NAME, "pk_col1", "pk_col2", "pk_col3", "value_col");
    SpannerAsserts.assertThatStructs(largePkData).hasRows(1);
    ImmutableList<Struct> largeIdxData =
        resourceManager.readTableRecords(
            LARGE_IDX_TABLE_NAME, "pk_col", "idx_col1", "idx_col2", "value_col");
    SpannerAsserts.assertThatStructs(largeIdxData).hasRows(1);
  }
}
