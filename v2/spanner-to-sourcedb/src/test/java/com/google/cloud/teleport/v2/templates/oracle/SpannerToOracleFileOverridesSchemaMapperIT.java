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
package com.google.cloud.teleport.v2.templates.oracle;

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
import org.apache.beam.it.jdbc.OracleResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for SpannerToSourceDb Flex template using file-based schema overrides for
 * Oracle.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToOracleFileOverridesSchemaMapperIT extends SpannerToSourceDbITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToOracleFileOverridesSchemaMapperIT.class);
  private static final HashSet<SpannerToOracleFileOverridesSchemaMapperIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  public static OracleResourceManager oracleResourceManager;
  public static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/SpannerToOracleFileOverridesSchemaMapperIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";
  private static final String ORACLE_SCHEMA_FILE_RESOURCE =
      "oracle/SpannerToOracleFileOverridesSchemaMapperIT/oracle-schema.sql";
  private static final String SCHEMA_OVERRIDE_FILE_RESOURCE =
      "oracle/SpannerToOracleFileOverridesSchemaMapperIT/file-overrides.json";
  private static final String SCHEMA_OVERRIDE_GCS_PREFIX = "SpannerToOracleFileOverridesIT";

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerToOracleFileOverridesSchemaMapperIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();
        oracleResourceManager = SharedOracleReverseITContainer.getInstance();
        testUsername = setupOracleIsolatedUser(oracleResourceManager);
        createOracleSchema(oracleResourceManager, ORACLE_SCHEMA_FILE_RESOURCE, testUsername);

        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadShardConfigToGcs(gcsResourceManager, oracleResourceManager);

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

        // For Oracle, we might need jdbcDriverJars, let's see if base class does this or if we add
        // it.
        // SKILL.md mentions doing it if needed. For now assuming not needed unless it fails or we
        // find it.
        // wait, Oracle uses proprietary driver usually, let me add a dummy check or leave it for
        // compilation to complain.

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
                "oracle", // source type
                jobParameters);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToOracleFileOverridesSchemaMapperIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testSpannerToOracleWithFileOverrides() throws Exception {
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
                    (runIsolatedGetRowCount(
                                oracleResourceManager, testUsername, "\"source_table1\"")
                            == 2
                        && runIsolatedGetRowCount(
                                oracleResourceManager, testUsername, "\"source_table2\"")
                            == 2));
    assertThatResult(result).meetsConditions();

    // Assert Oracle table1 (should be source_table1, with column name_col1 renamed)
    List<Map<String, Object>> oracleTable1 =
        runIsolatedSQLQuery(
            oracleResourceManager,
            testUsername,
            "SELECT \"id_col1\", \"name_col1\", TO_CHAR(\"data_col1\") AS \"data_col1\" FROM \"source_table1\" ORDER BY \"id_col1\" ASC");
    assertThat(oracleTable1).hasSize(2);
    // Note: Oracle might return BigDecimal for INT columns depending on JDBC mapping, using
    // toString().
    assertThat(oracleTable1.get(0).get("id_col1").toString()).isEqualTo("1");
    assertThat(oracleTable1.get(0).get("name_col1")).isEqualTo("Name One");
    assertThat(oracleTable1.get(0).get("data_col1")).isEqualTo("Data for one");
    assertThat(oracleTable1.get(1).get("id_col1").toString()).isEqualTo("2");
    assertThat(oracleTable1.get(1).get("name_col1")).isEqualTo("Name Two");
    assertThat(oracleTable1.get(1).get("data_col1")).isEqualTo("Data for two");

    // Assert Oracle table2 (should be source_table2, with column category_col2 renamed)
    List<Map<String, Object>> oracleTable2 =
        runIsolatedSQLQuery(
            oracleResourceManager,
            testUsername,
            "SELECT \"key_col2\", \"category_col2\", TO_CHAR(\"value_col2\") AS \"value_col2\" FROM \"source_table2\" ORDER BY \"key_col2\" ASC");
    assertThat(oracleTable2).hasSize(2);
    assertThat(oracleTable2.get(0).get("key_col2")).isEqualTo("K1");
    assertThat(oracleTable2.get(0).get("category_col2")).isEqualTo("Category Alpha");
    assertThat(oracleTable2.get(0).get("value_col2")).isEqualTo("Value Alpha");
    assertThat(oracleTable2.get(1).get("key_col2")).isEqualTo("K2");
    assertThat(oracleTable2.get(1).get("category_col2")).isEqualTo("Category Beta");
    assertThat(oracleTable2.get(1).get("value_col2")).isEqualTo("Value Beta");
  }
}
