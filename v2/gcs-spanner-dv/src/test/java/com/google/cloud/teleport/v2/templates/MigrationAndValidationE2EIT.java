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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.MismatchedRecordDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.dataflow.FlexTemplateClient;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-End Integration test for the full Spanner migration lifecycle.
 * Executes SourceDbToSpanner (with GCS outputs enabled) followed by GCSSpannerDV
 * to guarantee dynamic handoff works end-to-end for standard GoogleSQL runs.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class MigrationAndValidationE2EIT extends GCSSpannerDVITBase {

  private static final Logger LOG = LoggerFactory.getLogger(MigrationAndValidationE2EIT.class);

  private static final String SPANNER_DDL_RESOURCE =
      "MigrationAndValidationE2EIT/spanner-schema.sql"; 
  private static final String MYSQL_DDL_RESOURCE =
      "MigrationAndValidationE2EIT/mysql-schema.sql";

  private CloudMySQLResourceManager mySQLResourceManager;
  private PipelineLauncher bulkLauncher;

  @Before
  public void setUp() throws IOException {

    mySQLResourceManager = CloudMySQLResourceManager.builder(testName).build();
    
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);

    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    bulkLauncher = FlexTemplateClient.builder(credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(mySQLResourceManager);
    // Spanner and BigQuery are automatically cleaned up in tearDownBase()
  }

  @Test
  public void migrationAndValidationE2E() throws Exception {
    // 1. Generate and Upload Source Records to MySQL
    String mysqlSql = com.google.common.io.Resources.toString(
        com.google.common.io.Resources.getResource(MYSQL_DDL_RESOURCE), 
        java.nio.charset.StandardCharsets.UTF_8);
    for (String stmt : mysqlSql.split(";")) {
      if (!stmt.trim().isEmpty()) {
        mySQLResourceManager.runSQLUpdate(stmt);
      }
    }

    // Insert records into source MySQL DB
    List<Map<String, Object>> usersData = new ArrayList<>();
    usersData.add(Map.of("user_id", 1L, "event_id", "E1", "full_name", "Alice", "age", 30, "created_at", "2024-01-01 10:00:00"));
    usersData.add(Map.of("user_id", 2L, "event_id", "E2", "full_name", "Bob", "age", 31, "created_at", "2024-01-02 10:00:00"));
    usersData.add(Map.of("user_id", 4L, "event_id", "E4", "full_name", "David", "age", 35, "created_at", "2024-01-04 10:00:00"));
    mySQLResourceManager.write("Users", usersData);

    List<Map<String, Object>> rolesData = new ArrayList<>();
    rolesData.add(Map.of("role_id", 1L, "role_name", "ADMIN"));
    rolesData.add(Map.of("role_id", 2L, "role_name", "USER"));
    rolesData.add(Map.of("role_id", 3L, "role_name", "GUEST"));
    mySQLResourceManager.write("AccountRoles", rolesData);

    // 2. Launch Bulk Pipeline (SourceDbToSpanner)
    LOG.info("Launching SourceDbToSpanner bulk migration job");
    
    String bulkSpecPath = System.getProperty(
        "bulkSpecPath",
        "gs://dataflow-templates-us-central1/latest/flex/Sourcedb_to_Spanner_Flex");

    String gcsOutputDirectory = getGcsPath("output");
    Map<String, String> bulkParams = new HashMap<>();
    bulkParams.put("sourceDbDialect", "MYSQL");
    bulkParams.put("sourceConfigURL", mySQLResourceManager.getUri() + "?useSSL=false&allowPublicKeyRetrieval=true");
    bulkParams.put("username", mySQLResourceManager.getUsername());
    bulkParams.put("password", mySQLResourceManager.getPassword());
    bulkParams.put("jdbcDriverClassName", "com.mysql.jdbc.Driver");
    bulkParams.put("instanceId", spannerResourceManager.getInstanceId());
    bulkParams.put("databaseId", spannerResourceManager.getDatabaseId());
    bulkParams.put("outputDirectory", gcsOutputDirectory);
    bulkParams.put("gcsOutputDirectory", gcsOutputDirectory);
    bulkParams.put("projectId", PROJECT);
    bulkParams.put("workerMachineType", "n2-standard-4");

    LaunchConfig.Builder bulkOptions = LaunchConfig.builder(PipelineUtils.createJobName("bulk"), bulkSpecPath);
    bulkOptions.setParameters(bulkParams);
    bulkOptions.addEnvironment("additionalExperiments", List.of("disable_runner_v2"));
    bulkOptions.addEnvironment("numWorkers", 2);

    FlexTemplateClient bulkLauncher = FlexTemplateClient.builder(credentials).build();
    LaunchInfo bulkJobInfo = bulkLauncher.launch(PROJECT, REGION, bulkOptions.build());
    assertThatPipeline(bulkJobInfo).isRunning();

    PipelineOperator.Result bulkResult = pipelineOperator().waitUntilDone(createConfig(bulkJobInfo));
    assertThatResult(bulkResult).isLaunchFinished();


    // 3. Assert on spanner rows to verify the bulk job was actually successful
    long usersCount = spannerResourceManager.getRowCount("Users");
    long rolesCount = spannerResourceManager.getRowCount("AccountRoles");
    if (usersCount != 3 || rolesCount != 3) {
      throw new RuntimeException("Bulk job failed to migrate all records. Users: " + usersCount + ", Roles: " + rolesCount);
    }


    // 4. Manipulate Spanner data to create mismatches for the validation pipeline

    
    // Delete User 2 (MISSING_IN_DESTINATION)
    // Insert User 3 (MISSING_IN_SOURCE)
    // Update User 4 (MISMATCHED_VALUE)
    spannerResourceManager.write(
        Arrays.asList(
            Mutation.delete("Users", Key.of(2L, "E2")),
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("user_id").to(3L)
                .set("event_id").to("E3")
                .set("full_name").to("Charlie")
                .set("age").to(32L)
                .set("created_at").to(com.google.cloud.Timestamp.parseTimestamp("2024-01-03T10:00:00Z"))
                .build(),
            Mutation.newUpdateBuilder("Users")
                .set("user_id").to(4L)
                .set("event_id").to("E4")
                .set("age").to(40L)
                .build()));
    // Wait for Spanner's 20-second exact staleness read bound in SpannerReaderTransform

    Thread.sleep(20000);

    // 5. Launch Validation Pipeline (GCSSpannerDV)

    LaunchConfig.Builder dvOptions = LaunchConfig.builder(testName, specPath);
    LaunchInfo dvJobInfo =
        launchDataflowJob(
            dvOptions,
            testName,
            PROJECT,
            spannerResourceManager,
            bigQueryResourceManager.getDatasetId(),
            gcsOutputDirectory,
            null,
            null,
            null,
            null,
            null,
            null);

    pipelineOperator().waitUntilDone(createConfig(dvJobInfo));


    // 6. Assert BigQuery Validation Results
    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        Arrays.asList(
            new ValidationSummaryDto(
                /* status= */ "MISMATCH",
                /* totalTablesValidated= */ 2L,
                /* totalRowsMatched= */ 4L,
                /* totalRowsMismatched= */ 4L,
                /* tablesWithMismatches= */ "Users")));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "Users",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ 3L,
                /* destinationRowCount= */ 3L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 4L),
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "AccountRoles",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 3L,
                /* destinationRowCount= */ 3L,
                /* matchedRowCount= */ 3L,
                /* mismatchRowCount= */ 0L)));

    GCSSpannerDVTestAsserts.assertMismatchedRecords(
        bigQueryResourceManager,
        Arrays.asList(
            new MismatchedRecordDto(
                null, null, "Users", "[user_id:2, event_id:E2]", "MISSING_IN_DESTINATION"),
            new MismatchedRecordDto(
                null, null, "Users", "[user_id:4, event_id:E4]", "MISSING_IN_DESTINATION"),
            new MismatchedRecordDto(
                null, null, "Users", "[user_id:3, event_id:E3]", "MISSING_IN_SOURCE"),
            new MismatchedRecordDto(
                null, null, "Users", "[user_id:4, event_id:E4]", "MISSING_IN_SOURCE")));
  }
}
