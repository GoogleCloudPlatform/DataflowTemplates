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

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.constants.Constants;
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link SpannerToSourceDb} Flex template replicating from Spanner to Spanner
 * where the metadatadb and target Spanner are on different instances.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSpannerCrossDbIT extends SpannerToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSpannerCrossDbIT.class);

  private static final Duration TEST_TIMEOUT = Duration.ofMinutes(15);

  private static final String SPANNER_DDL_RESOURCE = "SpannerToSourceDbIT/spanner-schema.sql";

  private static final String TABLE = "Users";

  private static final HashSet<SpannerToSpannerCrossDbIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  private static SpannerResourceManager spannerResourceManager; // Source Spanner
  private static SpannerResourceManager spannerDestinationResourceManager; // Destination Spanner
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerToSpannerCrossDbIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);

        spannerDestinationResourceManager =
            SpannerResourceManager.builder("rr-dest-" + testName, PROJECT, REGION)
                .maybeUseStaticInstance()
                .build();
        createSpannerDDL(spannerDestinationResourceManager, SPANNER_DDL_RESOURCE);

        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        gcsResourceManager = setUpSpannerITGcsResourceManager();

        String spannerShardConfig =
            "["
                + "  {"
                + "    \"projectId\": \""
                + PROJECT
                + "\","
                + "    \"instanceId\": \""
                + spannerDestinationResourceManager.getInstanceId()
                + "\","
                + "    \"databaseId\": \""
                + spannerDestinationResourceManager.getDatabaseId()
                + "\""
                + "  }"
                + "]";
        gcsResourceManager.createArtifact("input/spanner-shard.json", spannerShardConfig);

        pubsubResourceManager = setUpPubSubResourceManager();
        SubscriptionName subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager)
                    .replace("gs://" + gcsResourceManager.getBucket(), ""),
                gcsResourceManager);

        Map<String, String> jobParameters = new HashMap<>();
        jobParameters.put(
            "sourceShardsFilePath", getGcsPath("input/spanner-shard.json", gcsResourceManager));

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
                Constants.SOURCE_SPANNER,
                jobParameters);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToSpannerCrossDbIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerDestinationResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void spannerToSpannerBasicReplication() throws InterruptedException {
    assertThatPipeline(jobInfo).isRunning();

    Mutation m =
        Mutation.newInsertOrUpdateBuilder(TABLE)
            .set("id")
            .to(101)
            .set("full_name")
            .to("Alice")
            .set("from")
            .to("London")
            .build();
    spannerResourceManager.write(m);
    LOG.info("Successfully wrote row to source Spanner Users table");

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                () -> {
                  List<Struct> rows =
                      spannerDestinationResourceManager.readTableRecords(
                          TABLE, List.of("id", "full_name", "from"));
                  for (Struct row : rows) {
                    if (!row.isNull("id")
                        && row.getLong("id") == 101
                        && !row.isNull("full_name")
                        && "Alice".equals(row.getString("full_name"))) {
                      LOG.info(
                          "Row successfully found in destination Spanner: id=101, full_name=Alice");
                      return true;
                    }
                  }
                  return false;
                });

    assertThatResult(result).meetsConditions();
  }
}
