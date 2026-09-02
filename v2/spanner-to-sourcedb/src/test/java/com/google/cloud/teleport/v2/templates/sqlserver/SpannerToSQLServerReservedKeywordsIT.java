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
import org.apache.beam.it.jdbc.MSSQLResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * An integration test for {@link SpannerToSourceDb} Flex template which tests a basic migration on
 * a simple schema with reserved keywords targeting SQL Server.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSQLServerReservedKeywordsIT extends SpannerToSourceDbITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/SpannerToSQLServerReservedKeywordsIT/spanner-schema.sql";
  private static final String SQLSERVER_DDL_RESOURCE =
      "sqlserver/SpannerToSQLServerReservedKeywordsIT/sqlserver-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "sqlserver/SpannerToSQLServerReservedKeywordsIT/session.json";

  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static MSSQLResourceManager mssqlResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
    spannerMetadataResourceManager = createSpannerMetadataDatabase();
    mssqlResourceManager = setUpMSSQLResourceManager(testName);
    createSQLServerSchema(mssqlResourceManager, SQLSERVER_DDL_RESOURCE);
    gcsResourceManager = setUpSpannerITGcsResourceManager();
    createAndUploadShardConfigToGcs(gcsResourceManager, mssqlResourceManager);
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
            SOURCE_SQLSERVER,
            jobParameters);
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        mssqlResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testSpannerToSQLServerReservedKeywords() throws InterruptedException {
    assertThatPipeline(jobInfo).isRunning();
    spannerResourceManager.write(generateData());
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> mssqlResourceManager.getRowCount("[true]") == 2);
    assertThatResult(result).meetsConditions();

    List<Map<String, Object>> rows = mssqlResourceManager.readTable("[true]");
    assertThat(rows).hasSize(2);
    rows.sort(Comparator.comparing(r -> (Long) r.get("COLUMN")));

    assertThat(rows.get(0).get("COLUMN")).isEqualTo(1L);
    assertThat(rows.get(0).get("TABLE")).isEqualTo("table1");
    assertThat(rows.get(0).get("WITH")).isEqualTo("with1");

    assertThat(rows.get(1).get("COLUMN")).isEqualTo(2L);
    assertThat(rows.get(1).get("TABLE")).isEqualTo("table2");
    assertThat(rows.get(1).get("WITH")).isEqualTo("with2");
  }

  private List<Mutation> generateData() {
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertOrUpdateBuilder("true")
            .set("COLUMN")
            .to(1)
            .set("TABLE")
            .to("table1")
            .set("WITH")
            .to("with1")
            .build());
    mutations.add(
        Mutation.newInsertOrUpdateBuilder("true")
            .set("COLUMN")
            .to(2)
            .set("TABLE")
            .to("table2")
            .set("WITH")
            .to("with2")
            .build());
    return mutations;
  }
}
