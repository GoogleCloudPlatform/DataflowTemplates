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

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.apache.beam.it.cassandra.CassandraResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * An integration test for {@link SpannerToSourceDb} Flex template which tests a basic migration on
 * a simple schema with reserved keywords.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDbReservedKeywordsIT extends SpannerToSourceDbITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToSourceDbReservedKeywordsIT/spanner-schema.sql";
  private static final String MYSQL_DDL_RESOURCE =
      "SpannerToSourceDbReservedKeywordsIT/mysql-schema.sql";
  private static final String CASSANDRA_DDL_RESOURCE =
      "SpannerToSourceDbReservedKeywordsIT/cassandra-schema.cql";

  private SpannerResourceManager spannerResourceManager;
  private MySQLResourceManager mySQLResourceManager;
  private CassandraResourceManager cassandraResourceManager;

  @Before
  public void setUp() {
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, mySQLResourceManager, cassandraResourceManager);
  }

  @Test
  public void testSpannerToMySqlReservedKeywords() throws IOException {
    mySQLResourceManager = setUpMySQLResourceManager();
    spannerResourceManager.executeDdlFile(SPANNER_DDL_RESOURCE);
    List<Mutation> expectedData = generateData();
    spannerResourceManager.write(expectedData);
    mySQLResourceManager.executeScriptFile(MYSQL_DDL_RESOURCE);

    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "spanner-to-mysql-reserved-keywords",
            spannerResourceManager,
            mySQLResourceManager,
            null);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));

    List<Map<String, Object>> actualData = mySQLResourceManager.readTable("true");
    assertEquals(expectedData.size(), actualData.size());
  }

  @Test
  public void testSpannerToCassandraReservedKeywords() throws IOException {
    cassandraResourceManager = setUpCassandraResourceManager();
    spannerResourceManager.executeDdlFile(SPANNER_DDL_RESOURCE);
    List<Mutation> expectedData = generateData();
    spannerResourceManager.write(expectedData);
    cassandraResourceManager.executeScript(CASSANDRA_DDL_RESOURCE);

    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "spanner-to-cassandra-reserved-keywords",
            spannerResourceManager,
            null,
            cassandraResourceManager);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));

    List<Map<String, Object>> actualData = cassandraResourceManager.readTable("true");
    assertEquals(expectedData.size(), actualData.size());
  }

  private List<Mutation> generateData() {
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      mutations.add(
          Mutation.newInsertOrUpdateBuilder("true")
              .set("COLUMN")
              .to(i)
              .set("TABLE")
              .to("value" + i)
              .set("WITH")
              .to("value" + i)
              .build());
    }
    return mutations;
  }
}
