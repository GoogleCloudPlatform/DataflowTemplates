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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
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
import org.junit.Ignore;

/**
 * An integration test for {@link SpannerToSourceDb} Flex template which tests a basic migration on
 * a simple schema with reserved keywords.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
@Ignore("Temporarily disabled for maintenance")
public class SpannerToMySqlReservedKeywordsIT extends SpannerToSourceDbITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToMySqlReservedKeywordsIT/spanner-schema.sql";
  private static final String MYSQL_DDL_RESOURCE =
      "SpannerToMySqlReservedKeywordsIT/mysql-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "SpannerToMySqlReservedKeywordsIT/session.json";

  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static MySQLResourceManager mySQLResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
    spannerMetadataResourceManager = createSpannerMetadataDatabase();
    mySQLResourceManager = MySQLResourceManager.builder(testName).build();
    createMySQLSchema(mySQLResourceManager, MYSQL_DDL_RESOURCE);
    gcsResourceManager = setUpSpannerITGcsResourceManager();
    createAndUploadShardConfigToGcs(gcsResourceManager, mySQLResourceManager);
    gcsResourceManager.uploadArtifact(
        "input/session.json", Resources.getResource(SESSION_FILE_RESOURCE).getPath());
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
            put("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager));
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
            MYSQL_SOURCE_TYPE,
            jobParameters);
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        mySQLResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testSpannerToMySqlReservedKeywords() throws InterruptedException {
    assertThatPipeline(jobInfo).isRunning();
    spannerResourceManager.write(generateData());
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> mySQLResourceManager.getRowCount("`true`") == 2);
    assertThatResult(result).meetsConditions();

    List<Map<String, Object>> actualData = mySQLResourceManager.readTable("`true`");
    List<Map<String, Object>> expectedData = getExpectedMySqlRows();

    // Sort both lists by the primary key for deterministic comparison
    actualData.sort(Comparator.comparing(m -> (Long) m.get("COLUMN")));
    expectedData.sort(Comparator.comparing(m -> (Long) m.get("COLUMN")));

    assertThat(actualData).isEqualTo(expectedData);
  }

  private List<Mutation> generateData() {
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertOrUpdateBuilder("true")
            .set("COLUMN")
            .to(1)
            .set("TABLE")
            .to("value1")
            .set("WITH")
            .to("value1")
            .build());
    mutations.add(
        Mutation.newInsertOrUpdateBuilder("true")
            .set("COLUMN")
            .to(2)
            .set("TABLE")
            .to("value2")
            .set("WITH")
            .to("value2")
            .build());
    return mutations;
  }

  private List<Map<String, Object>> getExpectedMySqlRows() {
    List<Map<String, Object>> rows = new ArrayList<>();
    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLUMN", 1L);
    row1.put("TABLE", "value1");
    row1.put("WITH", "value1");
    rows.add(row1);
    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLUMN", 2L);
    row2.put("TABLE", "value2");
    row2.put("WITH", "value2");
    rows.add(row2);
    return rows;
  }
}
