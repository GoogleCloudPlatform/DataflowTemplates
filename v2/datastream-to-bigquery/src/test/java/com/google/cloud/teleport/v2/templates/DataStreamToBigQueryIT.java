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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager.Builder;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

@TemplateIntegrationTest(DataStreamToBigQuery.class)
@RunWith(JUnit4.class)
public class DataStreamToBigQueryIT extends TemplateTestBase {
  private MySQLResourceManager jdbcResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private DatastreamResourceManager datastreamResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  private static final int ROW_COUNT = 10;

  private static final String ID = "id";
  private static final String JOB = "job";
  private static final String NAME = "name";

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    Builder datastreamResourceManagerBuilder =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider);
    String privateConnectivity = System.getProperty("privateConnectivity");
    if (privateConnectivity != null) {
      datastreamResourceManagerBuilder.setPrivateConnectivity(privateConnectivity);
    } else {
      datastreamResourceManagerBuilder.setPrivateConnectivity(
          "datastream-private-connect-us-central1");
    }
    datastreamResourceManager = datastreamResourceManagerBuilder.build();
    jdbcResourceManager = MySQLResourceManager.builder(testName).build();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        jdbcResourceManager,
        pubsubResourceManager,
        datastreamResourceManager,
        bigQueryResourceManager);
  }

  @Ignore("Flaky test")
  @Test
  public void testDataStreamToBigQuery() throws IOException {
    baseDataStreamToBigQuery(Function.identity());
  }

  @Ignore("Flaky test")
  @Test
  public void testDataStreamToBigQueryUsingAtleastOnceMode() throws IOException {
    ArrayList<String> experiments = new ArrayList<>();
    experiments.add("streaming_mode_at_least_once");
    baseDataStreamToBigQuery(
        b ->
            b.addEnvironment("additionalExperiments", experiments)
                .addEnvironment("enableStreamingEngine", true));
  }

  private void baseDataStreamToBigQuery(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder) throws IOException {
    // Arrange
    // Create a BigQuery table
    List<Field> bqSchemaFields =
        Arrays.asList(
            Field.of(ID, StandardSQLTypeName.INT64),
            Field.of(JOB, StandardSQLTypeName.STRING),
            Field.of(NAME, StandardSQLTypeName.STRING));
    Schema bqSchema = Schema.of(bqSchemaFields);
    bigQueryResourceManager.createDataset(REGION);
    TableId bqTable = bigQueryResourceManager.createTable(testName, bqSchema);

    // Create a Pub/Sub subscription
    String nameSuffix = RandomStringUtils.randomAlphanumeric(8);
    TopicName topic = pubsubResourceManager.createTopic("input-" + nameSuffix);
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, "sub-1-" + nameSuffix);

    // Create a JDBC table
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ID, "NUMERIC NOT NULL");
    columns.put(JOB, "VARCHAR(200)");
    columns.put(NAME, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ID);
    String tableName = testName;
    jdbcResourceManager.createTable(tableName, schema);

    // Setup Pub/Sub notifications on the GCS bucket
    String gcsPrefix = getGcsPath(testName).replace("gs://" + artifactBucketName, "") + "/cdc/";
    gcsClient.createNotification(topic.toString(), gcsPrefix);

    // Create Datastream JDBC Source Connection profile and config
    MySQLSource jdbcSource =
        MySQLSource.builder(
                jdbcResourceManager.getHost(),
                jdbcResourceManager.getUsername(),
                jdbcResourceManager.getPassword(),
                jdbcResourceManager.getPort())
            .build();
    SourceConfig sourceConfig =
        datastreamResourceManager.buildJDBCSourceConfig("mysql-profile", jdbcSource);

    // Create Datastream GCS Destination Connection profile and config
    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs-profile",
            artifactBucketName,
            gcsPrefix,
            DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT);

    // Create and start Datastream stream
    Stream stream =
        datastreamResourceManager.createStream("stream1", sourceConfig, destinationConfig);
    datastreamResourceManager.startStream(stream);

    // Construct the template
    String jobName = PipelineUtils.createJobName(testName);
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(jobName, specPath)
                .addParameter("inputFilePattern", getGcsPath(testName) + "/cdc/")
                .addParameter("inputFileFormat", "avro")
                .addParameter("gcsPubSubSubscription", subscription.toString())
                .addParameter("streamName", stream.getName())
                .addParameter(
                    "outputStagingDatasetTemplate", bigQueryResourceManager.getDatasetId())
                .addParameter("outputDatasetTemplate", bigQueryResourceManager.getDatasetId())
                .addParameter("outputTableNameTemplate", bqTable.getTable())
                .addParameter("deadLetterQueueDirectory", getGcsPath(testName) + "/dlq/"));
    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // Write records to the JDBC table
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 1; i <= ROW_COUNT; i++) {
      Map<String, Object> values = new HashMap<>();
      values.put(ID, i);
      values.put(JOB, jobName);
      values.put(NAME, RandomStringUtils.randomAlphabetic(10));
      rows.add(values);
    }
    jdbcResourceManager.write(tableName, rows);

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionsAndFinish(
                createConfig(info),
                BigQueryRowsCheck.builder(bigQueryResourceManager, bqTable)
                    .setMinRows(ROW_COUNT)
                    .build());

    // Assert
    assertThatResult(result).meetsConditions();
  }
}
