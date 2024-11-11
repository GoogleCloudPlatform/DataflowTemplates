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
package com.google.cloud.teleport.v2.templates;

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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests a basic migration on
 * a simple schema.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class PostgreSQLReservedKeywordsIT extends SourceDbToSpannerITBase {
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static PostgresResourceManager postgresSQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  // Spanner keywords from
  // https://cloud.google.com/spanner/docs/reference/standard-sql/lexical#reserved_keywords
  // PostgreSQL keywords from
  // https://github.com/postgres/postgres/blob/REL_15_STABLE/src/backend/parser/gram.y#L17073
  private static final String POSTGRESQL_DDL_RESOURCE = "ReservedKeywordsIT/postgresql-schema.sql";
  private static final String SPANNER_DDL_RESOURCE = "ReservedKeywordsIT/spanner-schema.sql";

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class. \
   */
  @Before
  public void setUp() {
    postgresSQLResourceManager = setUpPostgreSQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, postgresSQLResourceManager);
  }

  @Test
  public void reservedKeywordsTest() throws Exception {
    loadSQLFileResource(postgresSQLResourceManager, POSTGRESQL_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            postgresSQLResourceManager,
            spannerResourceManager,
            null,
            null);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));

    ImmutableList<Struct> reservedKeywordsSpanner =
        spannerResourceManager.readTableRecords("reserved_keywords", "id");
    SpannerAsserts.assertThatStructs(reservedKeywordsSpanner).hasRows(1);

    ImmutableList<Struct> reservedKeywordsPkSpanner =
        spannerResourceManager.readTableRecords("reserved_keywords_pk", "id");
    SpannerAsserts.assertThatStructs(reservedKeywordsPkSpanner).hasRows(1);
  }
}
