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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.secretmanager.SecretManagerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance tests for {@link DataStreamToSpanner} DataStream to Spanner template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpanner100GbLT extends DataStreamToSpannerLTBase {

  private static final String SPEC_PATH =
      "gs://dataflow-templates/latest/flex/Cloud_Datastream_to_Spanner";
  private final String artifactBucket = TestProperties.artifactBucket();
  private final String testRootDir = DataStreamToSpanner100GbLT.class.getSimpleName();
  private final String spannerDdlResource = "DataStreamToSpanner100GbLT/spanner-schema.sql";
  private final String table = "person";
  private final int maxWorkers = 100;
  private final int numWorkers = 50;
  public PubsubResourceManager pubsubResourceManager;
  public SpannerResourceManager spannerResourceManager;
  private GcsResourceManager gcsResourceManager;
  public DatastreamResourceManager datastreamResourceManager;
  private SecretManagerResourceManager secretClient;

  /**
   * Setup resource managers.
   *
   * @throws IOException
   */
  @Before
  public void setUpResourceManagers() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, project, region)
            .maybeUseStaticInstance()
            .setNodeCount(10)
            .build();
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, project, CREDENTIALS_PROVIDER).build();

    gcsResourceManager =
        GcsResourceManager.builder(artifactBucket, testRootDir, CREDENTIALS).build();
    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, project, region)
            .setCredentialsProvider(CREDENTIALS_PROVIDER)
            .build();
    secretClient = SecretManagerResourceManager.builder(project, CREDENTIALS_PROVIDER).build();
  }

  /**
   * Cleanup resource managers.
   *
   * @throws IOException
   */
  @After
  public void cleanUp() throws IOException {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager,
        datastreamResourceManager);
  }

  @Test
  public void backfill100Gb() throws IOException, ParseException, InterruptedException {
    // Setup resources
    createSpannerDDL(spannerResourceManager, spannerDdlResource);

    // TestClassName/runId/TestMethodName/cdc
    String gcsPrefix =
        String.join("/", new String[] {testRootDir, gcsResourceManager.runId(), testName, "cdc"});
    SubscriptionName subscription =
        createPubsubResources(
            testRootDir + testName, pubsubResourceManager, gcsPrefix, gcsResourceManager);

    String dlqGcsPrefix =
        String.join("/", new String[] {testRootDir, gcsResourceManager.runId(), testName, "dlq"});
    SubscriptionName dlqSubscription =
        createPubsubResources(
            testRootDir + testName + "dlq",
            pubsubResourceManager,
            dlqGcsPrefix,
            gcsResourceManager);

    // Setup Datastream
    MySQLSource mySQLSource = getMySQLSource();
    Stream stream =
        createDatastreamResources(
            artifactBucket, gcsPrefix, mySQLSource, datastreamResourceManager);

    // Setup Parameters
    Map<String, String> params =
        new HashMap<>() {
          {
            put("inputFilePattern", getGcsPath(artifactBucket, gcsPrefix));
            put("streamName", stream.getName());
            put("instanceId", spannerResourceManager.getInstanceId());
            put("databaseId", spannerResourceManager.getDatabaseId());
            put("projectId", project);
            put("deadLetterQueueDirectory", getGcsPath(artifactBucket, dlqGcsPrefix));
            put("gcsPubSubSubscription", subscription.toString());
            put("dlqGcsPubSubSubscription", dlqSubscription.toString());
            put("datastreamSourceType", "mysql");
            put("inputFileFormat", "avro");
          }
        };

    LaunchConfig.Builder options = LaunchConfig.builder(getClass().getSimpleName(), SPEC_PATH);
    options.addEnvironment("maxWorkers", maxWorkers).addEnvironment("numWorkers", numWorkers);

    options.setParameters(params);

    // Act
    PipelineLauncher.LaunchInfo jobInfo = pipelineLauncher.launch(project, region, options.build());
    assertThatPipeline(jobInfo).isRunning();

    ConditionCheck[] checks = new ConditionCheck[10];
    for (int i = 0; i < 10; ++i) {
      checks[i] =
          SpannerRowsCheck.builder(spannerResourceManager, table + (i + 1))
              .setMinRows(6500000)
              .setMaxRows(6500000)
              .build();
    }

    PipelineOperator.Result result =
        pipelineOperator.waitForCondition(
            createConfig(jobInfo, Duration.ofHours(4), Duration.ofMinutes(5)), checks);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    result = pipelineOperator.cancelJobAndFinish(createConfig(jobInfo, Duration.ofMinutes(20)));
    assertThatResult(result).isLaunchFinished();

    // export results
    exportMetricsToBigQuery(jobInfo, getMetrics(jobInfo));
  }

  public MySQLSource getMySQLSource() {
    String hostIp =
        secretClient.accessSecret(
            "projects/269744978479/secrets/nokill-datastream-mysql-to-spanner-cloudsql-ip-address/versions/1");
    String username =
        secretClient.accessSecret(
            "projects/269744978479/secrets/nokill-datastream-mysql-to-spanner-cloudsql-username/versions/1");
    String password =
        secretClient.accessSecret(
            "projects/269744978479/secrets/nokill-datastream-mysql-to-spanner-cloudsql-password/versions/1");
    MySQLSource mySQLSource = new MySQLSource.Builder(hostIp, username, password, 3306).build();
    return mySQLSource;
  }
}
