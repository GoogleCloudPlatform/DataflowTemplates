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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests a migration from a
 * PostgreSQL database containing multi-level table inheritance.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class PostgreSQLInheritanceTablesIT extends SourceDbToSpannerITBase {

  private static boolean initialized = false;
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static PostgresResourceManager postgresSQLResourceManager;
  public static SpannerResourceManager gsqlSpannerResourceManager;
  public static SpannerResourceManager pgDialectSpannerResourceManager;

  private static final String POSTGRESQL_DDL_RESOURCE = "InheritanceTablesIT/postgresql-schema.sql";
  private static final String SPANNER_GSQL_DDL_RESOURCE =
      "InheritanceTablesIT/spanner-gsql-schema.sql";
  private static final String SPANNER_PG_DDL_RESOURCE = "InheritanceTablesIT/spanner-pg-schema.sql";

  @Before
  public void setUp() throws Exception {
    synchronized (PostgreSQLInheritanceTablesIT.class) {
      if (!initialized) {
        postgresSQLResourceManager = setUpPostgreSQLResourceManager();
        gsqlSpannerResourceManager = setUpSpannerResourceManager();
        pgDialectSpannerResourceManager = setUpPGDialectSpannerResourceManager();

        loadSQLFileResource(postgresSQLResourceManager, POSTGRESQL_DDL_RESOURCE);

        initialized = true;
      }
    }
  }

  @AfterClass
  public static void cleanUp() {
    ResourceManagerUtils.cleanResources(
        postgresSQLResourceManager, gsqlSpannerResourceManager, pgDialectSpannerResourceManager);
  }

  @Test
  public void testPostgreSQLInheritanceGoogleSQLDialect() throws Exception {
    createSpannerDDL(gsqlSpannerResourceManager, SPANNER_GSQL_DDL_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName() + "GSQL",
            null,
            null,
            postgresSQLResourceManager,
            gsqlSpannerResourceManager,
            new HashMap<>(),
            null);

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    verifyData(gsqlSpannerResourceManager);
  }

  @Test
  public void testPostgreSQLInheritancePostgreSQLDialect() throws Exception {
    createSpannerDDL(pgDialectSpannerResourceManager, SPANNER_PG_DDL_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName() + "PG",
            null,
            null,
            postgresSQLResourceManager,
            pgDialectSpannerResourceManager,
            new HashMap<>(),
            null);

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    verifyData(pgDialectSpannerResourceManager);
  }

  private void verifyData(SpannerResourceManager spannerResourceManager) {
    List<Map<String, Object>> vehiclesPostgreSQL =
        postgresSQLResourceManager.runSQLQuery("SELECT id, make FROM ONLY vehicles");
    ImmutableList<Struct> vehiclesSpanner =
        spannerResourceManager.readTableRecords("vehicles", "id", "make");
    SpannerAsserts.assertThatStructs(vehiclesSpanner)
        .hasRecordsUnorderedCaseInsensitiveColumns(vehiclesPostgreSQL);

    List<Map<String, Object>> carsPostgreSQL =
        postgresSQLResourceManager.runSQLQuery("SELECT id, make, doors FROM ONLY cars");
    ImmutableList<Struct> carsSpanner =
        spannerResourceManager.readTableRecords("cars", "id", "make", "doors");
    SpannerAsserts.assertThatStructs(carsSpanner)
        .hasRecordsUnorderedCaseInsensitiveColumns(carsPostgreSQL);

    List<Map<String, Object>> sportsCarsPostgreSQL =
        postgresSQLResourceManager.runSQLQuery(
            "SELECT id, make, doors, top_speed FROM sports_cars");
    ImmutableList<Struct> sportsCarsSpanner =
        spannerResourceManager.readTableRecords("sports_cars", "id", "make", "doors", "top_speed");
    SpannerAsserts.assertThatStructs(sportsCarsSpanner)
        .hasRecordsUnorderedCaseInsensitiveColumns(sportsCarsPostgreSQL);
  }
}
