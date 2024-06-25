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
import com.google.common.io.Resources;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests a sharded migration.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class ShardedMigrationIT extends SourceDbToSpannerITBase {
  private static final Logger LOG = LoggerFactory.getLogger(ShardedMigrationIT.class);
  private static final HashSet<ShardedMigrationIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static MySQLResourceManager mysqlShard1;
  public static MySQLResourceManager mysqlShard2;
  public static SpannerResourceManager spannerResourceManager;

  private static final String SESSION_FILE_RESOURCE = "SchemaMapperIT/company-session.json";
  private static final String MYSQL_DDL_RESOURCE = "SchemaMapperIT/company-mysql-schema.sql";
  private static final String SPANNER_DDL_RESOURCE = "SchemaMapperIT/company-spanner-schema.sql";

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class. \
   */
  @Before
  public void setUp() {
    // mysqlShard1 = ShardedMySQLResourceManager.builder(testName + "shard_1", 3306).build();
    // TODO Integration tests don't give a clean way to spawn multiple mysql instances.
    // This needs to be built.
    // mysqlShard2 = ShardedMySQLResourceManager.builder(testName + "shard_2", 3307).build();
    mysqlShard1 = MySQLResourceManager.builder(testName + "shard1").build();
    mysqlShard2 = MySQLResourceManager.builder(testName + "shard2").build();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, mysqlShard1, mysqlShard2);
  }

  @Test
  public void noTransformationTest() throws Exception {
    loadSQLFileResource(mysqlShard1, MYSQL_DDL_RESOURCE);
    loadSQLFileResource(mysqlShard2, MYSQL_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    Map<String, String> jobParameters = new HashMap<>();
    jobParameters.put("tables", "employee");

    String gcsPathPrefix = "mapper";
    String shardConfigPath = gcsPathPrefix + "/shard-config.json";
    gcsClient.uploadArtifact(
        shardConfigPath, Resources.getResource("two-shard-config.json").getPath());
    jobParameters.put("sourceDbURL", getGcsPath(shardConfigPath));

    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            SESSION_FILE_RESOURCE,
            gcsPathPrefix,
            mysqlShard1, // This config is not used in this flow
            spannerResourceManager,
            jobParameters);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));

    List<Map<String, Object>> employeeMySQL1 =
        mysqlShard1.runSQLQuery(
            "SELECT employee_id, company_id, employee_name, employee_address FROM employee");
    for (Map<String, Object> employee : employeeMySQL1) {
      employee.put("migration_shard_id", "shard1");
    }
    List<Map<String, Object>> employeeMySQL2 =
        mysqlShard2.runSQLQuery(
            "SELECT employee_id, company_id, employee_name, employee_address FROM employee");
    for (Map<String, Object> employee : employeeMySQL2) {
      employee.put("migration_shard_id", "shard2");
    }

    ImmutableList<Struct> employeeSpanner =
        spannerResourceManager.readTableRecords(
            "employee",
            "employee_id",
            "company_id",
            "employee_name",
            "employee_address",
            "migration_shard_id");

    SpannerAsserts.assertThatStructs(employeeSpanner)
        .hasRecordsUnorderedCaseInsensitiveColumns(employeeMySQL1);
    SpannerAsserts.assertThatStructs(employeeSpanner)
        .hasRecordsUnorderedCaseInsensitiveColumns(employeeMySQL2);
    // SpannerAsserts.assertThatStructs(employeeSpanner)
    //     .hasRows(employeeMySQL1.size() + employeeMySQL2.size());
  }
}
