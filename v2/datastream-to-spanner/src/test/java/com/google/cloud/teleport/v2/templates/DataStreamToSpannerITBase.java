/*
 * Copyright (C) 2023 Google LLC
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

import com.google.auth.Credentials;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.IORedirectUtil;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for DataStreamToSpanner integration tests. It provides helper functions related to
 * environment setup and assertConditions.
 */
public abstract class DataStreamToSpannerITBase extends TemplateTestBase {

  // Format of avro file path in GCS - {table}/2023/12/20/06/57/{fileName}
  public static final String DATA_STREAM_EVENT_FILES_PATH_FORMAT_IN_GCS = "%s/2023/12/20/06/57/%s";
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToSpannerITBase.class);
  public static final int CUTOVER_MILLIS = 30 * 1000;

  protected static final Credentials CREDENTIALS = TestProperties.googleCredentials();

  public PubsubResourceManager setUpPubSubResourceManager() throws IOException {
    return PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  public SpannerResourceManager setUpSpannerResourceManager() {
    return SpannerResourceManager.builder(testName, PROJECT, REGION)
        .maybeUseStaticInstance()
        .build();
  }

  public GcsResourceManager setUpGCSResourceManager() {
    return GcsResourceManager.builder(testName.toLowerCase(), CREDENTIALS).build();
  }

  public SpannerResourceManager setUpShadowSpannerResourceManager() {
    // Create a separate spanner resource manager with different db name for shadow tables.
    SpannerResourceManager sp =
        SpannerResourceManager.builder("shadow_" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();
    // Set up the Spanner instance and database with the empty DDL for the resource manager.
    sp.ensureUsableAndCreateResources();
    return sp;
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
   * Helper function for constructing a ConditionCheck whose check() method uploads a given file to
   * GCS with path similar to DataStream generated file path.
   *
   * @param jobInfo Dataflow job info
   * @param table
   * @param destinationFileName
   * @param resourceName Avro file name with path relative to resources directory
   * @return A ConditionCheck containing the GCS Upload operation.
   */
  public ConditionCheck uploadDataStreamFile(
      LaunchInfo jobInfo,
      String table,
      String destinationFileName,
      String resourceName,
      GcsResourceManager gcsResourceManager) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Upload DataStream files.";
      }

      @Override
      protected CheckResult check() {
        boolean success = true;
        String message = String.format("Successfully uploaded %s file to GCS", resourceName);
        try {
          // Get destination GCS path from the dataflow job parameter.
          String destinationPath =
              jobInfo
                  .parameters()
                  .get("inputFilePattern")
                  .replace("gs://" + testName.toLowerCase() + "/", "");
          destinationPath =
              destinationPath
                  + String.format(
                      DATA_STREAM_EVENT_FILES_PATH_FORMAT_IN_GCS, table, destinationFileName);
          gcsResourceManager.copyFileToGcs(
              Paths.get(Resources.getResource(resourceName).getPath()), destinationPath);
        } catch (IOException e) {
          success = false;
          message = e.getMessage();
        }

        return new CheckResult(success, message);
      }
    };
  }

  /**
   * Performs the following steps: Uploads session file to GCS. Creates Pubsub resources. Launches
   * DataStreamToSpanner dataflow job.
   *
   * @param identifierSuffix will be used as postfix in generated resource ids
   * @param sessionFileResourceName Session file name with path relative to resources directory
   * @param transformationContextFileResourceName Transformation context file name with path
   *     relative to resources directory
   * @param gcsPathPrefix Prefix directory name for this DF job. Data and DLQ directories will be
   *     created under this prefix.
   * @return dataflow jobInfo object
   * @throws IOException
   */
  protected LaunchInfo launchDataflowJob(
      String identifierSuffix,
      String sessionFileResourceName,
      String transformationContextFileResourceName,
      String gcsPathPrefix,
      SpannerResourceManager spannerResourceManager,
      PubsubResourceManager pubsubResourceManager,
      GcsResourceManager gcsResourceManager,
      Map<String, String> jobParameters,
      CustomTransformation customTransformation,
      String shardingContextFileResourceName)
      throws IOException {

    if (sessionFileResourceName != null) {
      gcsResourceManager.uploadArtifact(
          "session.json", Resources.getResource(sessionFileResourceName).getPath());
    }

    if (transformationContextFileResourceName != null) {
      gcsResourceManager.uploadArtifact(
          "transformationContext.json",
          Resources.getResource(transformationContextFileResourceName).getPath());
    }

    if (shardingContextFileResourceName != null) {
      gcsResourceManager.uploadArtifact(
          "shardingContext.json", Resources.getResource(shardingContextFileResourceName).getPath());
    }

    SubscriptionName subscription =
        createPubsubResources(identifierSuffix, pubsubResourceManager, "cdc/", gcsResourceManager);
    SubscriptionName dlqSubscription =
        createPubsubResources(
            identifierSuffix + "dlq", pubsubResourceManager, "dlq/", gcsResourceManager);
    String gcsPath = "gs://" + testName.toLowerCase();

    // default parameters
    Map<String, String> params =
        new HashMap<>() {
          {
            put("inputFilePattern", gcsPath + "/cdc/");
            put("instanceId", spannerResourceManager.getInstanceId());
            put("databaseId", spannerResourceManager.getDatabaseId());
            put("projectId", PROJECT);
            put("deadLetterQueueDirectory", gcsPath + "/dlq/");
            put("gcsPubSubSubscription", subscription.toString());
            put("dlqGcsPubSubSubscription", dlqSubscription.toString());
            put("datastreamSourceType", "mysql");
            put("inputFileFormat", "avro");
          }
        };

    if (sessionFileResourceName != null) {
      params.put("sessionFilePath", gcsPath + "/session.json");
    }

    if (transformationContextFileResourceName != null) {
      params.put("transformationContextFilePath", gcsPath + "/transformationContext.json");
    }

    if (shardingContextFileResourceName != null) {
      params.put("shardingContextFilePath", gcsPath + "/shardingContext.json");
    }

    if (customTransformation != null) {
      params.put("transformationJarPath", gcsPath + "/" + customTransformation.jarPath());
      params.put("transformationClassName", customTransformation.classPath());
    }

    // overridden parameters
    if (jobParameters != null) {
      for (Entry<String, String> entry : jobParameters.entrySet()) {
        params.put(entry.getKey(), entry.getValue());
      }
    }

    // Construct template
    String jobName = PipelineUtils.createJobName(identifierSuffix);
    LaunchConfig.Builder options = LaunchConfig.builder(jobName, specPath);

    options.setParameters(params);

    // Run
    LaunchInfo jobInfo = launchTemplate(options, false);
    assertThatPipeline(jobInfo).isRunning();

    return jobInfo;
  }

  public void createAndUploadJarToGcs(GcsResourceManager gcsResourceManager, String gcsPathPrefix)
      throws IOException, InterruptedException {
    String[] shellCommand = {"/bin/bash", "-c", "cd ../spanner-custom-shard"};

    Process exec = Runtime.getRuntime().exec(shellCommand);

    IORedirectUtil.redirectLinesLog(exec.getInputStream(), LOG);
    IORedirectUtil.redirectLinesLog(exec.getErrorStream(), LOG);

    if (exec.waitFor() != 0) {
      throw new RuntimeException("Error staging template, check Maven logs.");
    }
    gcsResourceManager.uploadArtifact(
        gcsPathPrefix + "/customTransformation.jar",
        "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar");
  }
}
