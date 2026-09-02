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
 * Integration test for SpannerToSourceDb Flex template using file-based schema overrides targeting
 * SQL Server.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSQLServerFileOverridesSchemaMapperIT extends SpannerToSourceDbITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToSQLServerFileOverridesSchemaMapperIT.class);
  private static final HashSet<SpannerToSQLServerFileOverridesSchemaMapperIT> testInstances =
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
  private static final String SCHEMA_OVERRIDE_FILE_RESOURCE =
      "sqlserver/SpannerToSQLServerOverridesIT/file-overrides.json";
  private static final String SCHEMA_OVERRIDE_GCS_PREFIX = "SpannerToSQLServerOverridesIT";

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerToSQLServerFileOverridesSchemaMapperIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();
        mssqlResourceManager = setUpMSSQLResourceManager(testName);
        createSQLServerSchema(mssqlResourceManager, SQLSERVER_SCHEMA_FILE_RESOURCE);
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadShardConfigToGcs(gcsResourceManager, mssqlResourceManager);
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
        Map<String, String> jobParameters =
            new HashMap<>() {
              {
                put(
                    "schemaOverridesFilePath",
                    getGcsPath(
                        SCHEMA_OVERRIDE_GCS_PREFIX + "/file-overrides.json", gcsResourceManager));
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
    for (SpannerToSQLServerFileOverridesSchemaMapperIT instance : testInstances) {
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
  public void testSpannerToSQLServerFileOverrides() throws InterruptedException {
    assertThatPipeline(jobInfo).isRunning();

    Mutation m1 =
        Mutation.newInsertOrUpdateBuilder("Target_Table_1")
            .set("id_col1")
            .to(101)
            .set("Target_Name_Col_1")
            .to("override_name1")
            .set("data_col1")
            .to("override_data1")
            .build();
    Mutation m2 =
        Mutation.newInsertOrUpdateBuilder("source_table2")
            .set("key_col2")
            .to("override_key2")
            .set("Target_Category_Col_2")
            .to("override_category2")
            .set("value_col2")
            .to("override_value2")
            .build();
    spannerResourceManager.write(List.of(m1, m2));

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () ->
                    mssqlResourceManager.getRowCount("source_table1") == 1
                        && mssqlResourceManager.getRowCount("source_table2") == 1);
    assertThatResult(result).meetsConditions();

    List<Map<String, Object>> rows1 = mssqlResourceManager.readTable("source_table1");
    assertThat(rows1).hasSize(1);
    assertThat(rows1.get(0).get("id_col1")).isEqualTo(101);
    assertThat(rows1.get(0).get("name_col1")).isEqualTo("override_name1");
    assertThat(rows1.get(0).get("data_col1")).isEqualTo("override_data1");

    List<Map<String, Object>> rows2 = mssqlResourceManager.readTable("source_table2");
    assertThat(rows2).hasSize(1);
    assertThat(rows2.get(0).get("key_col2")).isEqualTo("override_key2");
    assertThat(rows2.get(0).get("category_col2")).isEqualTo("override_category2");
    assertThat(rows2.get(0).get("value_col2")).isEqualTo("override_value2");
  }
}
