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
 * PostgreSQL database containing partitioned tables.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class PostgreSQLPartitionedTablesIT extends SourceDbToSpannerITBase {

  private static boolean initialized = false;

  public static PostgresResourceManager postgresSQLResourceManager;
  public static SpannerResourceManager gsqlSpannerResourceManager;
  public static SpannerResourceManager pgDialectSpannerResourceManager;

  private static final String POSTGRESQL_DDL_RESOURCE = "PartitionedTablesIT/postgresql-schema.sql";
  private static final String SPANNER_GSQL_DDL_RESOURCE =
      "PartitionedTablesIT/spanner-gsql-schema.sql";
  private static final String SPANNER_PG_DDL_RESOURCE = "PartitionedTablesIT/spanner-pg-schema.sql";

  @Before
  public void setUp() throws Exception {
    synchronized (PostgreSQLPartitionedTablesIT.class) {
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
  public void testPostgreSQLPartitionedTablesGoogleSQLDialect() throws Exception {
    createSpannerDDL(gsqlSpannerResourceManager, SPANNER_GSQL_DDL_RESOURCE);
    PipelineLauncher.LaunchInfo jobInfo =
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
  public void testPostgreSQLPartitionedTablesPostgreSQLDialect() throws Exception {
    createSpannerDDL(pgDialectSpannerResourceManager, SPANNER_PG_DDL_RESOURCE);
    PipelineLauncher.LaunchInfo jobInfo =
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
    List<Map<String, Object>> measurementsPostgreSQL =
        postgresSQLResourceManager.runSQLQuery(
            "SELECT id, city_id, logdate, peaktemp FROM measurements_range");
    ImmutableList<Struct> measurementsSpanner =
        spannerResourceManager.readTableRecords(
            "measurements_range", "id", "city_id", "logdate", "peaktemp");
    SpannerAsserts.assertThatStructs(measurementsSpanner)
        .hasRecordsUnorderedCaseInsensitiveColumns(measurementsPostgreSQL);

    List<Map<String, Object>> employeesPostgreSQL =
        postgresSQLResourceManager.runSQLQuery("SELECT id, name, department FROM employees_list");
    ImmutableList<Struct> employeesSpanner =
        spannerResourceManager.readTableRecords("employees_list", "id", "name", "department");
    SpannerAsserts.assertThatStructs(employeesSpanner)
        .hasRecordsUnorderedCaseInsensitiveColumns(employeesPostgreSQL);

    List<Map<String, Object>> ordersPostgreSQL =
        postgresSQLResourceManager.runSQLQuery(
            "SELECT order_id, customer_id, amount FROM orders_hash");
    ImmutableList<Struct> ordersSpanner =
        spannerResourceManager.readTableRecords("orders_hash", "order_id", "customer_id", "amount");
    SpannerAsserts.assertThatStructs(ordersSpanner)
        .hasRecordsUnorderedCaseInsensitiveColumns(ordersPostgreSQL);
  }
}
