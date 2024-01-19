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

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
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
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager.DestinationOutputFormat;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.jdbc.CustomMySQLResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;

/**
 * Base class for DataStreamToSpanner integration tests. It provides helper functions related to
 * environment setup and assertConditions.
 */
public abstract class DataStreamToSpannerITBase extends TemplateTestBase {

  // Format of avro file path in GCS - {table}/2023/12/20/06/57/{fileName}
  public static final String DATA_STREAM_EVENT_FILES_PATH_FORMAT_IN_GCS = "%s/2023/12/20/06/57/%s";

  public static final String SESSION_FILE_PATH_IN_GCS = "input/mysql_session.json";

  public PubsubResourceManager setUpPubSubResourceManager() throws IOException {
    return PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  public SpannerResourceManager setUpSpannerResourceManager() {
    return SpannerResourceManager.builder(testName, PROJECT, REGION)
        .maybeUseStaticInstance()
        .build();
  }

  public DatastreamResourceManager setUpDataStreamResourceManager() throws IOException {
    return DatastreamResourceManager.builder(testName, PROJECT, REGION)
        .setCredentialsProvider(credentialsProvider)
        .setPrivateConnectivity("datastream-private-connect-us-central1")
        .build();
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
   * Helper function for creating schema in source DB.
   *
   * @param schemaMap Map with table name as Key and table's schema as
   *     JDBCResourceManager.JDBCSchema object
   * @param jdbcResourceManager
   */
  public void createSourceSchema(
      Map<String, JDBCResourceManager.JDBCSchema> schemaMap,
      JDBCResourceManager jdbcResourceManager) {
    if (schemaMap != null) {
      for (Entry<String, JDBCResourceManager.JDBCSchema> schema : schemaMap.entrySet()) {
        jdbcResourceManager.createTable(schema.getKey(), schema.getValue());
      }
    }
  }

  /**
   * Helper function for creating all DataStream resources required by DataStreamToSpanner template.
   * Source connection profile, Destination connection profile, Create and start Stream.
   *
   * @param datastreamResourceManager Initialized PubSubResourceManager instance
   * @param sourceDatabase Initialized CustomMySQLResourceManager instance
   */
  public Stream createDataStreamResources(
      DatastreamResourceManager datastreamResourceManager,
      CustomMySQLResourceManager sourceDatabase,
      DestinationOutputFormat fileFormat) {
    MySQLSource mySQLSource =
        MySQLSource.builder(
                sourceDatabase.getHost(),
                sourceDatabase.getUsername(),
                sourceDatabase.getPassword(),
                sourceDatabase.getPort())
            .build();

    // Create Datastream JDBC Source Connection profile and config
    SourceConfig sourceConfig =
        datastreamResourceManager.buildJDBCSourceConfig("mysql-profile", mySQLSource);

    // Create Datastream GCS Destination Connection profile and config
    String gcsPrefix = getGcsPath("cdc/").replace("gs://" + artifactBucketName, "");
    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs-profile", artifactBucketName, gcsPrefix, fileFormat);

    // Create and start Datastream stream
    Stream stream =
        datastreamResourceManager.createStream("stream1", sourceConfig, destinationConfig);
    datastreamResourceManager.startStream(stream);

    return stream;
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
      PubsubResourceManager pubsubResourceManager, String gcsPrefix) {
    String topicNameSuffix = "it";
    String subscriptionNameSuffix = "simple-it-sub";
    TopicName topic = pubsubResourceManager.createTopic(topicNameSuffix);
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, subscriptionNameSuffix);
    String prefix = gcsPrefix;
    if (prefix.startsWith("/")) {
      prefix = prefix.substring(1);
    }
    gcsClient.createNotification(topic.toString(), prefix);
    return subscription;
  }

  /**
   * Helper function for constructing a ConditionCheck whose check() method applies DML statements
   * on the JDBC database from the given sql file resource.
   *
   * @return A ConditionCheck containing the JDBC mutate operation.
   */
  public ConditionCheck applyDML(JDBCResourceManager jdbcResourceManager, String resourceName) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Send initial JDBC events.";
      }

      @Override
      protected CheckResult check() {
        String dml = null;
        try {
          dml =
              String.join(
                  " ",
                  Resources.readLines(Resources.getResource(resourceName), StandardCharsets.UTF_8));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        dml = dml.trim();
        String[] dmls = dml.split(";");
        for (String d : dmls) {
          if (!d.isBlank()) {
            jdbcResourceManager.runSQLUpdate(d);
          }
        }

        return new CheckResult(true, String.format("Sent %d updates to source DB.", dmls.length));
      }
    };
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
      LaunchInfo jobInfo, String table, String destinationFileName, String resourceName) {
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
                  .replace("gs://" + artifactBucketName + "/", "");
          destinationPath =
              destinationPath
                  + String.format(
                      DATA_STREAM_EVENT_FILES_PATH_FORMAT_IN_GCS, table, destinationFileName);
          gcsClient.copyFileToGcs(
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
   * Performs the following steps: Uploads session file to GCS. Created schema in Spanner by reading
   * DDL from spanner-schema.sql file. Creates Pubsub resources. Launches DataStreamToSpanner
   * dataflow job.
   *
   * @param testName will be used as postfix in generated resource ids
   * @param sessionFileResourceName Session file name with path relative to resources directory
   * @param spannerSchemaResourceName spanner schema file name with path relative to resources
   *     directory
   * @return dataflow jobInfo object
   * @throws IOException
   */
  protected LaunchInfo launchDataflowJob(
      String testName,
      String sessionFileResourceName,
      String spannerSchemaResourceName,
      SpannerResourceManager spannerResourceManager,
      PubsubResourceManager pubsubResourceManager,
      Map<String, String> jobParameters)
      throws IOException {

    gcsClient.uploadArtifact(
        SESSION_FILE_PATH_IN_GCS, Resources.getResource(sessionFileResourceName).getPath());

    createSpannerDDL(spannerResourceManager, spannerSchemaResourceName);

    String gcsPrefix = getGcsPath("cdc/").replace("gs://" + artifactBucketName, "");
    SubscriptionName subscription = createPubsubResources(pubsubResourceManager, gcsPrefix);

    // default parameters
    Map<String, String> params =
        new HashMap<>() {
          {
            put("inputFilePattern", getGcsPath("cdc/"));
            put(
                "streamName",
                String.format(
                    "projects/%s/locations/us-central1/streams/test-stream-name", PROJECT));
            put("instanceId", spannerResourceManager.getInstanceId());
            put("databaseId", spannerResourceManager.getDatabaseId());
            put("projectId", PROJECT);
            put("deadLetterQueueDirectory", getGcsPath("dlq/"));
            put("sessionFilePath", getGcsPath(SESSION_FILE_PATH_IN_GCS));
            put("gcsPubSubSubscription", subscription.toString());
            put("datastreamSourceType", "mysql");
            put("inputFileFormat", "avro");
          }
        };

    // overridden parameters
    if (jobParameters != null) {
      for (Entry<String, String> entry : jobParameters.entrySet()) {
        params.put(entry.getKey(), entry.getValue());
      }
    }

    // Construct template
    String jobName = PipelineUtils.createJobName(testName);
    LaunchConfig.Builder options = LaunchConfig.builder(jobName, specPath);

    options.setParameters(params);

    // Run
    LaunchInfo jobInfo = launchTemplate(options, false);
    assertThatPipeline(jobInfo).isRunning();

    return jobInfo;
  }
}
