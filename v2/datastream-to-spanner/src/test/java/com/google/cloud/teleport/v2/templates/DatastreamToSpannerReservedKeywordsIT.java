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

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * An integration test for {@link DatastreamToSpanner} Flex template which tests a basic migration
 * on a simple schema with reserved keywords.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DatastreamToSpanner.class)
@RunWith(JUnit4.class)
public class DatastreamToSpannerReservedKeywordsIT extends DatastreamToSpannerITBase {

  private static final String MYSQL_DDL_RESOURCE = "ReservedKeywordsIT/mysql-schema.sql";
  private static final String POSTGRESQL_DDL_RESOURCE = "ReservedKeywordsIT/postgresql-schema.sql";
  private static final String SPANNER_DDL_RESOURCE = "ReservedKeywordsIT/spanner-schema.sql";

  private MySQLResourceManager mySQLResourceManager;
  private PostgresResourceManager postgresResourceManager;
  private SpannerResourceManager spannerResourceManager;

  @Before
  public void setUp() {
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        mySQLResourceManager, postgresResourceManager, spannerResourceManager);
  }

  @Test
  public void testMySqlReservedKeywords() throws Exception {
    mySQLResourceManager = setUpMySQLResourceManager();
    mySQLResourceManager.executeScriptFile(MYSQL_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    // More setup required for datastream which is out of scope for this test
  }

  @Test
  public void testPostgresReservedKeywords() throws Exception {
    postgresResourceManager = setUpPostgresResourceManager();
    postgresResourceManager.executeScriptFile(POSTGRESQL_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    // More setup required for datastream which is out of scope for this test
  }
}
