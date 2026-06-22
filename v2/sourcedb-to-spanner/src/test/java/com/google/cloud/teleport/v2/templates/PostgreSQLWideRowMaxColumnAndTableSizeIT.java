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

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class PostgreSQLWideRowMaxColumnAndTableSizeIT extends SourceDbToSpannerITBase {

  private static boolean initialized = false;
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static PostgresResourceManager postgresResourceManager;
  public static SpannerResourceManager gsqlSpannerResourceManager;
  public static SpannerResourceManager pgDialectSpannerResourceManager;

  private static final String POSTGRESQL_DUMP_FILE_RESOURCE =
      "WideRow/PostgreSQLMaxColumnAndTableSize/postgresql-schema.sql";
  private static final String SPANNER_SCHEMA_FILE_RESOURCE =
      "WideRow/PostgreSQLMaxColumnAndTableSize/gsql-spanner-schema.sql";
  private static final String PG_DIALECT_SPANNER_SCHEMA_FILE_RESOURCE =
      "WideRow/PostgreSQLMaxColumnAndTableSize/pg-dialect-spanner-schema.sql";

  private static final String TABLE =
      "testtable_03tpcovf16ed0klxm3v808ch3btgq0uk_fexuzhbttvyzpaegeqio";

  @Before
  public void setUp() throws Exception {
    synchronized (PostgreSQLWideRowMaxColumnAndTableSizeIT.class) {
      if (!initialized) {
        postgresResourceManager = setUpPostgreSQLResourceManager();
        gsqlSpannerResourceManager = setUpSpannerResourceManager();
        pgDialectSpannerResourceManager = setUpPGDialectSpannerResourceManager();

        loadSQLFileResource(postgresResourceManager, POSTGRESQL_DUMP_FILE_RESOURCE);

        initialized = true;
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    ResourceManagerUtils.cleanResources(
        postgresResourceManager, gsqlSpannerResourceManager, pgDialectSpannerResourceManager);
  }

  @Test
  public void wideRowMaxColumnAndTableSizeGSQLDialect() throws Exception {
    createSpannerDDL(gsqlSpannerResourceManager, SPANNER_SCHEMA_FILE_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName() + "GSQL",
            null,
            null,
            postgresResourceManager,
            gsqlSpannerResourceManager,
            null,
            null);

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    verifyData(gsqlSpannerResourceManager);
  }

  @Test
  public void wideRowMaxColumnAndTableSizePGDialect() throws Exception {
    createSpannerDDL(pgDialectSpannerResourceManager, PG_DIALECT_SPANNER_SCHEMA_FILE_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName() + "PG",
            null,
            null,
            postgresResourceManager,
            pgDialectSpannerResourceManager,
            null,
            null);

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    verifyData(pgDialectSpannerResourceManager);
  }

  private void verifyData(SpannerResourceManager resourceManager) {
    ImmutableList<Struct> wideRowData =
        resourceManager.readTableRecords(
            TABLE, "id", "col_qcbf69rmxtre3b_03tpcovf16ed0klxm3v808ch3btgq0uk_fexuzhbttvy");
    SpannerAsserts.assertThatStructs(wideRowData).hasRows(1);
  }
}
