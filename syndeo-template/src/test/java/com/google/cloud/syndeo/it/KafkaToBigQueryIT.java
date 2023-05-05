/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.syndeo.it;

import static com.google.cloud.syndeo.transforms.KafkaToBigQueryLocalTest.INTEGRATION_TEST_SCHEMA;
import static com.google.cloud.syndeo.transforms.KafkaToBigQueryLocalTest.generateBaseRootConfiguration;
import static com.google.cloud.syndeo.transforms.KafkaToBigQueryLocalTest.generateRow;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auth.Credentials;
import com.google.cloud.syndeo.SyndeoTemplate;
import com.google.cloud.syndeo.transforms.KafkaToBigQueryLocalTest;
import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineOperator;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.PipelineUtils;
import com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.dataflow.FlexTemplateClient;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.joda.time.Duration;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class KafkaToBigQueryIT {
  @Rule public final TestName testName = new TestName();

  private static final String PROJECT = TestProperties.project();
  private static final String REGION = TestProperties.region();
  private static final String TEMP_LOCATION = TestProperties.artifactBucket();
  private static final Credentials CREDENTIALS = TestProperties.googleCredentials();
  private static final String KAFKA_BOOTSTRAP_SERVER =
      System.getProperty("kafka_bootstrap_server", "kafka-pabloem-sept-2022-test-m:9092");
  private static final String KAFKA_TOPIC = "quickstart-events";
  private static final Integer KAFKA_TOPIC_PARTITIONS = 27;
  // The syndeo_test dataset exists in the cloud-teleport-testing project.
  private static final String BIGQUERY_DATASET =
      "syndeo_test" + UUID.randomUUID().toString().replaceAll("-", "");
  private static final String BIGQUERY_TABLE = BIGQUERY_DATASET + ".table" + UUID.randomUUID();
  private static final String SPEC_PATH =
      TestProperties.specPath() != null
          ? TestProperties.specPath()
          : "gs://cloud-teleport-testing-df-staging/syndeo-template.json";

  private static final Integer ELEMENTS_PER_SECOND = 1_000_000;
  private static final Integer PIPELINE_DURATION_SECONDS = 600;

  private static final Long ONE_MINUTE_MILLIS = 60 * 1000L;
  private static final Long SIX_MINUTES_MILLIS = 6 * ONE_MINUTE_MILLIS;

  private BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setUp() {
    bigQueryResourceManager = BigQueryResourceManager.builder("kafka-bq-test", PROJECT).build();
    bigQueryResourceManager.createDataset(REGION);
  }

  @Test
  public void testErrorOnCreateNeverIfTableNotExisting() throws Exception {
    String tableName =
        String.format("%s.%s.NONEXISTENT_TABLE", PROJECT, bigQueryResourceManager.getDatasetId());
    JsonNode rootConfig =
        KafkaToBigQueryLocalTest.generateConfigurationWithKafkaBootstrap(
            KAFKA_BOOTSTRAP_SERVER, tableName);
    ((ObjectNode) rootConfig.get("sink").get("configurationParameters"))
        .put("createDisposition", "CREATE_NEVER");
    RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () -> {
              PipelineResult result =
                  SyndeoTemplate.run(
                      new String[] {
                        "--jsonSpecPayload=" + rootConfig,
                        "--streaming",
                        "--experiments=use_deprecated_read",
                        // We need to set this option because otherwise the pipeline will block on
                        // p.run() and
                        // never
                        // reach Thread.sleep (and never be cancelled).
                        "--blockOnRun=false"
                      });
              result.cancel();
            });
    assertTrue(
        "Error message contains missing table name. " + e,
        e.getMessage().contains("NONEXISTENT_TABLE"));
  }

  @After
  public void tearDown() {
    bigQueryResourceManager.cleanupAll();
  }

  @Ignore
  @Test
  public void testOnlyWriteDataToKafka() throws Exception {
    // Make sure that Kafka Server exists
    // Start data generation pipeline
    PipelineResult dataGeneratorPipeline = kickstartDataGeneration();
    if (!PipelineUtils.waitUntilState(
        dataGeneratorPipeline, PipelineResult.State.RUNNING, SIX_MINUTES_MILLIS)) {
      dataGeneratorPipeline.cancel();
      throw new RuntimeException(
          "Data generator pipeline did not start by deadline. Cancelling test");
    }
  }

  @Ignore
  @Test
  public void testKickOffKafkaToBigQuerySyndeoPipeline() throws Exception {
    // Make sure that Kafka Server exists
    // Start data generation pipeline
    PipelineResult dataGeneratorPipeline = kickstartDataGeneration();
    if (!PipelineUtils.waitUntilState(
        dataGeneratorPipeline, PipelineResult.State.RUNNING, SIX_MINUTES_MILLIS)) {
      dataGeneratorPipeline.cancel();
      throw new RuntimeException(
          "Data generator pipeline did not start by deadline. Cancelling test");
    }

    // Start syndeo pipeline.
    FlexTemplateClient templateClient =
        FlexTemplateClient.builder().setCredentials(CREDENTIALS).build();
    PipelineOperator operator = new PipelineOperator(templateClient);
    {
      PipelineLauncher.LaunchInfo syndeoPipeline = kickstartSyndeoPipeline();

      // Wait for two minutes while the pipeline executes, and then drain it.
      operator.waitUntilDoneAndFinish(
          PipelineOperator.Config.builder()
              .setProject(PROJECT)
              .setRegion(REGION)
              .setJobId(syndeoPipeline.jobId())
              .setTimeoutAfter(java.time.Duration.ofMinutes(3))
              .build());
    }

    // Start a new syndeo pipeline. We do this to test for appropriate behavior on restarts
    PipelineLauncher.LaunchInfo syndeoPipeline = kickstartSyndeoPipeline();

    // Sleep while the pipeline runs to move all the data.
    Thread.sleep(3 * ONE_MINUTE_MILLIS);

    // Wait for a while to check BigQuery results
    {
      Instant start = Instant.now();
      while (true) {
        Long rowsInTable =
            bigQueryResourceManager.getRowCount(
                BIGQUERY_TABLE.substring(BIGQUERY_TABLE.lastIndexOf(".") + 1));
        if (rowsInTable == ELEMENTS_PER_SECOND * PIPELINE_DURATION_SECONDS) {
          // Test passes yay!
          break;
        } else if (rowsInTable > ELEMENTS_PER_SECOND * PIPELINE_DURATION_SECONDS) {
          throw new AssertionError(
              String.format(
                  "Expected at most %s elements to be inserted to BigQuery, but found %s.",
                  ELEMENTS_PER_SECOND * PIPELINE_DURATION_SECONDS, rowsInTable));
        } else if (java.time.Duration.between(start, Instant.now())
                .compareTo(java.time.Duration.ofMinutes(20))
            > 0) {
          // If we spent over 20 minutees waiting for the job, then we must exit
          throw new AssertionError(
              String.format(
                  "Expected %s elements to be inserted to BigQuery, but found %s after over 20 minutes.",
                  ELEMENTS_PER_SECOND * PIPELINE_DURATION_SECONDS, rowsInTable));
        } else {
          // Iterate once more
          Thread.sleep(ONE_MINUTE_MILLIS);
          continue;
        }
      }
    }

    // Wait three minutes while the pipeline drains
    operator.drainJobAndFinish(
        PipelineOperator.Config.builder()
            .setProject(PROJECT)
            .setRegion(REGION)
            .setJobId(syndeoPipeline.jobId())
            .setTimeoutAfter(java.time.Duration.ofMinutes(5))
            .build());
  }

  private void createKafkaTopic()
      throws ExecutionException, InterruptedException, TimeoutException {
    // TODO: Move this to a KafkaResourceManager.
    try (AdminClient adminClient =
        AdminClient.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER))) {
      Collection<NewTopic> topics =
          Collections.singletonList(new NewTopic(KAFKA_TOPIC, KAFKA_TOPIC_PARTITIONS, (short) 1));
      try {
        adminClient
            .deleteTopics(Collections.singletonList(KAFKA_TOPIC))
            .all()
            .get(30, TimeUnit.SECONDS);
      } catch (Exception e) {
        // Ignore this exception
      }
      adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
    }
  }

  PipelineLauncher.LaunchInfo kickstartSyndeoPipeline() throws Exception {
    JsonNode templateConfiguration = generateBaseRootConfiguration(null);

    ObjectNode sourceProps =
        ((ObjectNode) templateConfiguration.get("source").get("configurationParameters"));
    sourceProps.put("topic", KAFKA_TOPIC);
    sourceProps.put("bootstrapServers", KAFKA_BOOTSTRAP_SERVER);
    ObjectNode kafkaUpdates = (ObjectNode) sourceProps.get("consumerConfigUpdates");
    kafkaUpdates.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");

    ObjectNode sinkProps =
        ((ObjectNode) templateConfiguration.get("sink").get("configurationParameters"));
    sinkProps.put("table", BIGQUERY_TABLE);
    sinkProps.put("useTestingBigQueryServices", false);
    String jobName = "syndeo-job-" + UUID.randomUUID();

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder(jobName, SPEC_PATH)
            .addParameter("jsonSpecPayload", templateConfiguration.toString())
            .addParameter(
                "experiments", "enable_streaming_engine,enable_streaming_auto_sharding=true")
            .build();

    PipelineLauncher.LaunchInfo actual =
        FlexTemplateClient.builder()
            .setCredentials(CREDENTIALS)
            .build()
            .launch(PROJECT, REGION, options);

    return actual;
  }

  PipelineResult kickstartDataGeneration() {
    Pipeline p = Pipeline.create();
    p.getOptions().as(DataflowPipelineOptions.class).setRunner(DataflowRunner.class);
    p.getOptions().as(DataflowPipelineOptions.class).setProject(TestProperties.project());
    p.getOptions().as(DataflowPipelineOptions.class).setTempLocation(TEMP_LOCATION);
    p.getOptions().as(DataflowPipelineOptions.class).setRegion("us-central1");
    p.getOptions().as(DataflowPipelineOptions.class).setStreaming(true);

    p.apply(
            GenerateSequence.from(0)
                .to(PIPELINE_DURATION_SECONDS * ELEMENTS_PER_SECOND)
                .withRate(ELEMENTS_PER_SECOND, Duration.standardSeconds(1)))
        .apply(ParDo.of(new GenerateKVPairsForKafka(INTEGRATION_TEST_SCHEMA)))
        .apply(
            KafkaIO.<Long, byte[]>write()
                .withTopic(KAFKA_TOPIC)
                .withBootstrapServers(KAFKA_BOOTSTRAP_SERVER)
                .withKeySerializer(LongSerializer.class)
                .withValueSerializer(ByteArraySerializer.class));
    return p.run();
  }

  private static class GenerateKVPairsForKafka extends DoFn<Long, KV<Long, byte[]>> {
    private SerializableFunction<Row, byte[]> toAvroBytesFn;

    GenerateKVPairsForKafka(Schema rowSchema) {
      toAvroBytesFn =
          rowSchema == null
              ? AvroUtils.getRowToAvroBytesFunction(INTEGRATION_TEST_SCHEMA)
              : AvroUtils.getRowToAvroBytesFunction(rowSchema);
    }

    @ProcessElement
    public void process(@Element Long elm, OutputReceiver<KV<Long, byte[]>> receiver) {
      receiver.output(KV.of(elm, toAvroBytesFn.apply(generateRow())));
    }
  }
}
