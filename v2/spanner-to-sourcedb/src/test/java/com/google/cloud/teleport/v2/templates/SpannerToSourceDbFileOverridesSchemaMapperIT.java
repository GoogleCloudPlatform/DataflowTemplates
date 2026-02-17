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

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Ignore;

/** Integration test for SpannerToSourceDb Flex template using file-based schema overrides. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
@Ignore("Temporarily disabled for maintenance")
public class SpannerToSourceDbFileOverridesSchemaMapperIT extends SpannerToSourceDbITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToSourceDbFileOverridesSchemaMapperIT.class);
  private static final HashSet<SpannerToSourceDbFileOverridesSchemaMapperIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  public static MySQLResourceManager mySQLResourceManager;
  public static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToSourceDbOverridesIT/spanner-schema.sql";
  private static final String MYSQL_SCHEMA_FILE_RESOURCE =
      "SpannerToSourceDbOverridesIT/mysql-schema.sql";
  private static final String SCHEMA_OVERRIDE_FILE_RESOURCE =
      "SpannerToSourceDbOverridesIT/file-overrides.json";
  private static final String SCHEMA_OVERRIDE_GCS_PREFIX = "SpannerToSourceDbOverridesIT";

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerToSourceDbFileOverridesSchemaMapperIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();
        mySQLResourceManager = MySQLResourceManager.builder(testName).build();
        createMySQLSchema(mySQLResourceManager, MYSQL_SCHEMA_FILE_RESOURCE);
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadShardConfigToGcs(gcsResourceManager, mySQLResourceManager);
        gcsResourceManager.uploadArtifact(
            SCHEMA_OVERRIDE_GCS_PREFIX + "/file-overrides.json",
            Resources.getResource(SCHEMA_OVERRIDE_FILE_RESOURCE).getPath());
        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager)
                    .replace("gs://" + gcsResourceManager.getBucket(), ""),
                gcsResourceManager);
        Map<String, String> jobParameters = new HashMap<>();
        jobParameters.put(
            "schemaOverridesFilePath",
            getGcsPath(SCHEMA_OVERRIDE_GCS_PREFIX + "/file-overrides.json", gcsResourceManager));
        jobInfo =
            launchDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                subscriptionName.toString(),
                null,
                null,
                null,
                null,
                null,
                MYSQL_SOURCE_TYPE,
                jobParameters);
      }
    }
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToSourceDbFileOverridesSchemaMapperIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        mySQLResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testSpannerToMySQLWithFileOverrides() throws Exception {
    assertThatPipeline(jobInfo).isRunning();
    // Insert data into Spanner tables matching the override scenario
    spannerResourceManager.write(
        Mutation.newInsertOrUpdateBuilder("Target_Table_1")
            .set("id_col1")
            .to(1)
            .set("Target_Name_Col_1")
            .to("Name One")
            .set("data_col1")
            .to("Data for one")
            .build());
    spannerResourceManager.write(
        Mutation.newInsertOrUpdateBuilder("Target_Table_1")
            .set("id_col1")
            .to(2)
            .set("Target_Name_Col_1")
            .to("Name Two")
            .set("data_col1")
            .to("Data for two")
            .build());
    spannerResourceManager.write(
        Mutation.newInsertOrUpdateBuilder("source_table2")
            .set("key_col2")
            .to("K1")
            .set("Target_Category_Col_2")
            .to("Category Alpha")
            .set("value_col2")
            .to("Value Alpha")
            .build());
    spannerResourceManager.write(
        Mutation.newInsertOrUpdateBuilder("source_table2")
            .set("key_col2")
            .to("K2")
            .set("Target_Category_Col_2")
            .to("Category Beta")
            .set("value_col2")
            .to("Value Beta")
            .build());

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () ->
                    (mySQLResourceManager.getRowCount("source_table1") == 2
                        && mySQLResourceManager.getRowCount("source_table2") == 2));
    assertThatResult(result).meetsConditions();

    // Assert MySQL table1 (should be source_table1, with column name_col1 renamed)
    List<Map<String, Object>> mysqlTable1 =
        mySQLResourceManager.runSQLQuery("SELECT id_col1, name_col1, data_col1 FROM source_table1");
    assertThat(mysqlTable1).hasSize(2);
    assertThat(mysqlTable1.get(0).get("id_col1")).isEqualTo(1);
    assertThat(mysqlTable1.get(0).get("name_col1")).isEqualTo("Name One");
    assertThat(mysqlTable1.get(0).get("data_col1")).isEqualTo("Data for one");
    assertThat(mysqlTable1.get(1).get("id_col1")).isEqualTo(2);
    assertThat(mysqlTable1.get(1).get("name_col1")).isEqualTo("Name Two");
    assertThat(mysqlTable1.get(1).get("data_col1")).isEqualTo("Data for two");

    // Assert MySQL table2 (should be source_table2, with column category_col2 renamed)
    List<Map<String, Object>> mysqlTable2 =
        mySQLResourceManager.runSQLQuery(
            "SELECT key_col2, category_col2, value_col2 FROM source_table2");
    assertThat(mysqlTable2).hasSize(2);
    assertThat(mysqlTable2.get(0).get("key_col2")).isEqualTo("K1");
    assertThat(mysqlTable2.get(0).get("category_col2")).isEqualTo("Category Alpha");
    assertThat(mysqlTable2.get(0).get("value_col2")).isEqualTo("Value Alpha");
    assertThat(mysqlTable2.get(1).get("key_col2")).isEqualTo("K2");
    assertThat(mysqlTable2.get(1).get("category_col2")).isEqualTo("Category Beta");
    assertThat(mysqlTable2.get(1).get("value_col2")).isEqualTo("Value Beta");
  }
}
