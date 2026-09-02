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
package com.google.cloud.teleport.v2.templates.sqlserver;

import static com.google.cloud.teleport.v2.templates.constants.Constants.SOURCE_SQLSERVER;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
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
import org.apache.beam.it.jdbc.MSSQLResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for SpannerToSourceDb Flex template using string-based schema overrides
 * targeting SQL Server.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSQLServerStringOverridesSchemaMapperIT extends SpannerToSourceDbITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToSQLServerStringOverridesSchemaMapperIT.class);
  private static final HashSet<SpannerToSQLServerStringOverridesSchemaMapperIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  public static MSSQLResourceManager mssqlResourceManager;
  public static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/SpannerToSQLServerOverridesIT/spanner-schema.sql";
  private static final String SQLSERVER_SCHEMA_FILE_RESOURCE =
      "sqlserver/SpannerToSQLServerOverridesIT/sqlserver-schema.sql";

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerToSQLServerStringOverridesSchemaMapperIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();
        mssqlResourceManager = setUpMSSQLResourceManager(testName);
        createSQLServerSchema(mssqlResourceManager, SQLSERVER_SCHEMA_FILE_RESOURCE);
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadShardConfigToGcs(gcsResourceManager, mssqlResourceManager);
        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager)
                    .replace("gs://" + gcsResourceManager.getBucket(), ""),
                gcsResourceManager);
        Map<String, String> jobParameters =
            new HashMap<>() {
              {
                put("tableOverrides", "[{source_table1, Target_Table_1}]");
                put(
                    "columnOverrides",
                    "[{source_table1.name_col1, source_table1.Target_Name_Col_1}, {source_table2.category_col2, source_table2.Target_Category_Col_2}]");
              }
            };
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
                SOURCE_SQLSERVER,
                jobParameters);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToSQLServerStringOverridesSchemaMapperIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        mssqlResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testSpannerToSQLServerWithStringOverrides() throws Exception {
    assertThatPipeline(jobInfo).isRunning();
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
                    (mssqlResourceManager.getRowCount("source_table1") == 2
                        && mssqlResourceManager.getRowCount("source_table2") == 2));
    assertThatResult(result).meetsConditions();

    List<Map<String, Object>> sqlserverTable1 =
        mssqlResourceManager.runSQLQuery("SELECT id_col1, name_col1, data_col1 FROM source_table1");
    assertThat(sqlserverTable1).hasSize(2);
    assertThat(sqlserverTable1.get(0).get("id_col1")).isEqualTo(1);
    assertThat(sqlserverTable1.get(0).get("name_col1")).isEqualTo("Name One");
    assertThat(sqlserverTable1.get(0).get("data_col1")).isEqualTo("Data for one");
    assertThat(sqlserverTable1.get(1).get("id_col1")).isEqualTo(2);
    assertThat(sqlserverTable1.get(1).get("name_col1")).isEqualTo("Name Two");
    assertThat(sqlserverTable1.get(1).get("data_col1")).isEqualTo("Data for two");

    List<Map<String, Object>> sqlserverTable2 =
        mssqlResourceManager.runSQLQuery(
            "SELECT key_col2, category_col2, value_col2 FROM source_table2");
    assertThat(sqlserverTable2).hasSize(2);
    assertThat(sqlserverTable2.get(0).get("key_col2")).isEqualTo("K1");
    assertThat(sqlserverTable2.get(0).get("category_col2")).isEqualTo("Category Alpha");
    assertThat(sqlserverTable2.get(0).get("value_col2")).isEqualTo("Value Alpha");
    assertThat(sqlserverTable2.get(1).get("key_col2")).isEqualTo("K2");
    assertThat(sqlserverTable2.get(1).get("category_col2")).isEqualTo("Category Beta");
    assertThat(sqlserverTable2.get(1).get("value_col2")).isEqualTo("Value Beta");
  }
}
