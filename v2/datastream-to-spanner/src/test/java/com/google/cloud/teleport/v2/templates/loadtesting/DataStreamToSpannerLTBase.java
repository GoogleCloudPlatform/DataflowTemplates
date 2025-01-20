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
package com.google.cloud.teleport.v2.templates.loadtesting;

import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.CONVERSION_ERRORS_COUNTER_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.OTHER_PERMANENT_ERRORS_COUNTER_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.RETRYABLE_ERRORS_COUNTER_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.SKIPPED_EVENTS_COUNTER_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.SUCCESSFUL_EVENTS_COUNTER_NAME;
import static java.util.Arrays.stream;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.secretmanager.SecretManagerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.junit.After;

/**
 * Base class for DataStreamToSpanner Load tests. It provides helper functions related to
 * environment setup and assertConditions.
 */
public class DataStreamToSpannerLTBase extends TemplateLoadTestBase {
  protected static final String SPEC_PATH =
      "gs://dataflow-templates/latest/flex/Cloud_Datastream_to_Spanner";
  protected final String artifactBucket = TestProperties.artifactBucket();
  protected String testRootDir;
  protected final int maxWorkers = 100;
  protected final int numWorkers = 50;
  public PubsubResourceManager pubsubResourceManager;
  public SpannerResourceManager spannerResourceManager;
  protected GcsResourceManager gcsResourceManager;
  public DatastreamResourceManager datastreamResourceManager;
  protected SecretManagerResourceManager secretClient;

  /**
   * Setup resource managers.
   *
   * @throws IOException
   */
  public void setUpResourceManagers(String spannerDdlResource) throws IOException {
    testRootDir = getClass().getSimpleName();
    spannerResourceManager =
        SpannerResourceManager.builder(testName, project, region)
            .maybeUseStaticInstance()
            .setNodeCount(10)
            .setMonitoringClient(monitoringClient)
            .build();
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, project, CREDENTIALS_PROVIDER)
            .setMonitoringClient(monitoringClient)
            .build();

    gcsResourceManager =
        GcsResourceManager.builder(artifactBucket, testRootDir, CREDENTIALS).build();
    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, project, region)
            .setCredentialsProvider(CREDENTIALS_PROVIDER)
            .build();
    secretClient = SecretManagerResourceManager.builder(project, CREDENTIALS_PROVIDER).build();
    createSpannerDDL(spannerResourceManager, spannerDdlResource);
  }

  public void runLoadTest(HashMap<String, Integer> tables, JDBCSource mySQLSource)
      throws IOException, ParseException, InterruptedException {
    runLoadTest(tables, mySQLSource, new HashMap<>(), new HashMap<>());
  }

  public void runLoadTest(
      HashMap<String, Integer> tables,
      JDBCSource mySQLSource,
      HashMap<String, String> templateParameters,
      HashMap<String, Object> environmentOptions)
      throws IOException, ParseException, InterruptedException {
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
    // Add all parameters for the template
    params.putAll(templateParameters);

    LaunchConfig.Builder options = LaunchConfig.builder(getClass().getSimpleName(), SPEC_PATH);
    options.addEnvironment("maxWorkers", maxWorkers).addEnvironment("numWorkers", numWorkers);

    // Set all environment options
    environmentOptions.forEach((key, value) -> options.addEnvironment(key, value));
    options.setParameters(params);

    // Act
    PipelineLauncher.LaunchInfo jobInfo = pipelineLauncher.launch(project, region, options.build());
    assertThatPipeline(jobInfo).isRunning();

    ConditionCheck[] checks = new ConditionCheck[tables.size()];
    int iterationCount = 0;
    for (Map.Entry<String, Integer> entry : tables.entrySet()) {
      checks[iterationCount] =
          SpannerRowsCheck.builder(spannerResourceManager, entry.getKey())
              .setMinRows(entry.getValue())
              .setMaxRows(entry.getValue())
              .build();
      iterationCount++;
    }

    PipelineOperator.Result result =
        pipelineOperator.waitForCondition(
            createConfig(jobInfo, Duration.ofHours(4), Duration.ofMinutes(5)), checks);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    result = pipelineOperator.cancelJobAndFinish(createConfig(jobInfo, Duration.ofMinutes(20)));
    assertThatResult(result).isLaunchFinished();

    Map<String, Double> metrics = getMetrics(jobInfo);
    getCustomCounters(jobInfo, metrics);
    getResourceManagerMetrics(metrics);

    // export results
    exportMetricsToBigQuery(jobInfo, metrics);
  }

  public void getResourceManagerMetrics(Map<String, Double> metrics) {
    pubsubResourceManager.collectMetrics(metrics);
    spannerResourceManager.collectMetrics(metrics);
  }

  /**
   * Cleanup resource managers.
   *
   * @throws IOException
   */
  @After
  public void cleanUp() throws IOException {
    ResourceManagerUtils.cleanResources(
        secretClient,
        spannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager,
        datastreamResourceManager);
  }

  public MySQLSource getMySQLSource(String hostIp, String username, String password) {
    MySQLSource mySQLSource = new MySQLSource.Builder(hostIp, username, password, 3306).build();
    return mySQLSource;
  }

  /**
   * Helper function for creating all datastream resources required by DataStreamToSpanner template.
   * Source connection profile, Destination connection profile, Stream. And then Starts the stream.
   *
   * @param artifactBucketName
   * @param gcsPrefix
   * @param jdbcSource
   * @param datastreamResourceManager
   * @return created stream
   */
  public Stream createDatastreamResources(
      String artifactBucketName,
      String gcsPrefix,
      JDBCSource jdbcSource,
      DatastreamResourceManager datastreamResourceManager) {
    // To-do, for future postgres load testing, add a parameter to accept other sources
    SourceConfig sourceConfig =
        datastreamResourceManager.buildJDBCSourceConfig("mysql", jdbcSource);

    // Create Datastream GCS Destination Connection profile and config
    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs",
            artifactBucketName,
            gcsPrefix,
            DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT);

    // Create and start Datastream stream
    Stream stream =
        datastreamResourceManager.createStream("ds-spanner", sourceConfig, destinationConfig);
    datastreamResourceManager.startStream(stream);
    return stream;
  }

  /**
   * Helper function for creating Spanner DDL. Reads the sql file from resources directory and
   * applies the DDL to Spanner instance.
   *
   * @param spannerResourceManager Initialized SpannerResourceManager instance
   * @param resourceName SQL file name with path relative to resources directory
   */
  public void createSpannerDDL(SpannerResourceManager spannerResourceManager, String resourceName)
      throws IOException {
    String ddl =
        String.join(
            " ", Resources.readLines(Resources.getResource(resourceName), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    String[] ddls = ddl.split(";");
    for (String d : ddls) {
      if (!d.isBlank()) {
        spannerResourceManager.executeDdlStatement(d);
      }
    }
  }

  /**
   * Helper function for creating all pubsub resources required by DataStreamToSpanner template.
   * PubSub topic, Subscription and notification setup on a GCS bucket with gcsPrefix filter.
   *
   * @param pubsubResourceManager Initialized PubSubResourceManager instance
   * @param gcsPrefix Prefix of Avro file names in GCS relative to bucket name
   * @return SubscriptionName object of the created PubSub subscription.
   */
  public SubscriptionName createPubsubResources(
      String identifierSuffix,
      PubsubResourceManager pubsubResourceManager,
      String gcsPrefix,
      GcsResourceManager gcsResourceManager) {
    String topicNameSuffix = "it" + identifierSuffix;
    String subscriptionNameSuffix = "it-sub" + identifierSuffix;
    TopicName topic = pubsubResourceManager.createTopic(topicNameSuffix);
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, subscriptionNameSuffix);
    String prefix = gcsPrefix;
    if (prefix.startsWith("/")) {
      prefix = prefix.substring(1);
    }
    gcsResourceManager.createNotification(topic.toString(), prefix);
    return subscription;
  }

  /**
   * Returns the full GCS path given a list of path parts.
   *
   * <p>"path parts" refers to the bucket, directories, and file. Only the bucket is mandatory and
   * must be the first value provided.
   *
   * @param pathParts everything that makes up the path, minus the separators. There must be at
   *     least one value, and none of them can be empty
   * @return the full path, such as 'gs://bucket/dir1/dir2/file'
   */
  public String getGcsPath(String... pathParts) {
    checkArgument(pathParts.length != 0, "Must provide at least one path part");
    checkArgument(
        stream(pathParts).noneMatch(Strings::isNullOrEmpty), "No path part can be null or empty");

    return String.format("gs://%s", String.join("/", pathParts));
  }

  public Map<String, Double> getCustomCounters(LaunchInfo launchInfo, Map<String, Double> metrics)
      throws IOException {
    Double successfulEvents =
        pipelineLauncher.getMetric(
            project, region, launchInfo.jobId(), SUCCESSFUL_EVENTS_COUNTER_NAME);
    metrics.put(
        "Custom_Counter_SuccessfulEvents", successfulEvents != null ? successfulEvents : 0.0);
    Double retryableErrors =
        pipelineLauncher.getMetric(
            project, region, launchInfo.jobId(), RETRYABLE_ERRORS_COUNTER_NAME);
    metrics.put("Custom_Counter_RetryableErrors", retryableErrors != null ? retryableErrors : 0.0);

    Double permanentErrors = 0.0;
    Double skippedEvents =
        pipelineLauncher.getMetric(
            project, region, launchInfo.jobId(), SKIPPED_EVENTS_COUNTER_NAME);
    permanentErrors += skippedEvents != null ? skippedEvents : 0.0;
    Double otherPermanentErrors =
        pipelineLauncher.getMetric(
            project, region, launchInfo.jobId(), OTHER_PERMANENT_ERRORS_COUNTER_NAME);
    permanentErrors += otherPermanentErrors != null ? otherPermanentErrors : 0.0;
    Double conversionErrors =
        pipelineLauncher.getMetric(
            project, region, launchInfo.jobId(), CONVERSION_ERRORS_COUNTER_NAME);
    permanentErrors += conversionErrors != null ? conversionErrors : 0.0;

    metrics.put("Custom_Counter_PermanentErrors", permanentErrors);
    return metrics;
  }
}
