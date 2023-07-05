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
package com.google.cloud.syndeo.perf;

import static com.google.cloud.syndeo.transforms.SyndeoStatsSchemaTransformProvider.getElementsProcessed;
import static org.junit.Assert.fail;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auto.value.AutoValue;
import com.google.cloud.syndeo.SyndeoTemplate;
import com.google.cloud.syndeo.transforms.pubsub.SyndeoPubsubWriteSchemaTransformProvider;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.joda.time.Instant;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(TemplateLoadTest.class)
@RunWith(JUnit4.class)
public class SyndeoPubsubToBigQueryLT {

  private static final Logger LOG = LoggerFactory.getLogger(SyndeoPubsubToBigQueryLT.class);
  @Rule public TestPipeline dataGenerator = TestPipeline.create();

  @Rule public TestPipeline syndeoPipeline = TestPipeline.create();

  private static final String PROJECT = TestProperties.project();
  private static final String LOCATION = TestProperties.region();

  protected static final Credentials CREDENTIALS = TestProperties.googleCredentials();
  protected static final CredentialsProvider CREDENTIALS_PROVIDER =
      FixedCredentialsProvider.create(CREDENTIALS);
  private static final String TEST_CONFIG =
      TestProperties.getProperty("configuration", "local", TestProperties.Type.PROPERTY);

  // 30 characters is the maximum length for some literals, so we cap it here.
  private final String testUuid =
      ("syndeo-"
              + DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")
                  .withZone(ZoneId.of("UTC"))
                  .format(java.time.Instant.now())
              + UUID.randomUUID())
          .substring(0, 30);

  private PubsubResourceManager pubsub = null;
  private BigQueryResourceManager bigquery = null;
  private Instant testStart = null;

  @Before
  public void beforeTest() throws IOException {
    bigquery =
        BigQueryResourceManager.builder("syndeo-pubsub-bq-lt", PROJECT)
            .setCredentials(CREDENTIALS)
            .build();
    pubsub =
        PubsubResourceManager.builder("syndeo-pubsub-bq-lt", PROJECT)
            .credentialsProvider(CREDENTIALS_PROVIDER)
            .build();
    testStart = Instant.now();
  }

  @After
  public void afterTest() {
    ResourceManagerUtils.cleanResources(pubsub, bigquery);
  }

  @AutoValue
  abstract static class PubsubToBigQueryConfiguration {
    public abstract Long getNumRows();

    public abstract Integer getDurationMinutes();

    public abstract String getRunner();

    public static PubsubToBigQueryConfiguration create(
        Long throughput, Integer durationMinutes, String runner) {
      return new AutoValue_SyndeoPubsubToBigQueryLT_PubsubToBigQueryConfiguration(
          throughput, durationMinutes, runner);
    }
  }

  private static final PubsubToBigQueryConfiguration LOCAL_TEST_CONFIG =
      PubsubToBigQueryConfiguration.create(1_000L, 2, "DirectRunner");
  private static final PubsubToBigQueryConfiguration DATAFLOW_TEST_CONFIG =
      PubsubToBigQueryConfiguration.create(50_000_000L, 20, "DataflowRunner");
  private static final PubsubToBigQueryConfiguration LARGE_DATAFLOW_TEST_CONFIG =
      PubsubToBigQueryConfiguration.create(500_000_000L, 80, "DataflowRunner");

  private static final Map<String, PubsubToBigQueryConfiguration> TEST_CONFIGS =
      Map.of(
          "local",
          LOCAL_TEST_CONFIG,
          "medium",
          DATAFLOW_TEST_CONFIG,
          "large",
          LARGE_DATAFLOW_TEST_CONFIG);

  @Test
  public void testPubsubToBigQuerySyndeoFlow() throws IOException, InterruptedException {
    PubsubToBigQueryConfiguration testConfig = TEST_CONFIGS.get(TEST_CONFIG);
    if (testConfig == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown test configuration: [%s]. Known configs: %s",
              TEST_CONFIG, TEST_CONFIGS.keySet()));
    }
    String topicName = "syndeo-topic-test-" + testUuid;
    String subscriptionName = "syndeo-subscription-test-" + testUuid;

    TopicName topicPath = pubsub.createTopic(topicName);
    SubscriptionName subscriptionPath = pubsub.createSubscription(topicPath, subscriptionName);

    // Create BigQuery resources
    String tableName = "syndeo-bq-table-" + testUuid;
    bigquery.createDataset(LOCATION);

    PCollectionRowTuple.of(
            "input",
            SyndeoLoadTestUtils.inputData(
                dataGenerator,
                testConfig.getNumRows(),
                testConfig.getDurationMinutes() * 60L,
                SyndeoLoadTestUtils.NESTED_TABLE_SCHEMA))
        .apply(
            "syndeo:schematransform:com.google.cloud:pubsub_write:v1",
            new SyndeoPubsubWriteSchemaTransformProvider()
                .from(
                    SyndeoPubsubWriteSchemaTransformProvider.SyndeoPubsubWriteConfiguration.create(
                        "AVRO", topicPath.toString())));

    // Build JSON configuration for the template:
    String jsonPayload =
        SyndeoLoadTestUtils.mapToJsonPayload(
            Map.of(
                "source",
                Map.of(
                    "urn",
                    // "beam:schematransform:org.apache.beam:pubsub_read:v1",
                    "syndeo:schematransform:com.google.cloud:pubsub_read:v1",
                    "configurationParameters",
                    Map.of(
                        "subscription",
                        subscriptionPath.toString(),
                        "format",
                        "AVRO",
                        "schema",
                        AvroUtils.toAvroSchema(SyndeoLoadTestUtils.NESTED_TABLE_SCHEMA)
                            .toString())),
                "sink",
                Map.of(
                    "urn",
                    "beam:schematransform:org.apache.beam:bigquery_storage_write:v1",
                    "configurationParameters",
                    Map.of(
                        "table",
                        String.format("%s.%s.%s", PROJECT, bigquery.getDatasetId(), tableName),
                        "createDisposition",
                        "CREATE_IF_NEEDED",
                        "triggeringFrequencySeconds",
                        2))));

    SyndeoTemplate.buildPipeline(syndeoPipeline, SyndeoTemplate.buildFromJsonPayload(jsonPayload));

    PipelineResult generatorResult =
        dataGenerator.runWithAdditionalOptionArgs(
            List.of(
                "--runner=" + testConfig.getRunner(),
                "--experiments=enable_streaming_engine",
                "--blockOnRun=false"));

    PipelineResult syndeoResult =
        syndeoPipeline.runWithAdditionalOptionArgs(
            List.of(
                "--runner=" + testConfig.getRunner(),
                "--experiments=enable_streaming_engine,enable_streaming_auto_sharding=true",
                "--blockOnRun=false"));
    PipelineResult.State generatorOutcome = generatorResult.waitUntilFinish();
    if (generatorOutcome.equals(PipelineResult.State.FAILED)) {
      syndeoResult.cancel();
      fail("Data generator job failed to start. Unable to perform test.");
    }

    while (true) {
      Long inputElements =
          Math.min(getElementsProcessed(generatorResult.metrics()), testConfig.getNumRows());
      Long syndeoProcessed = getElementsProcessed(syndeoResult.metrics());
      int testRuntime = new Period(testStart, Instant.now()).toStandardMinutes().getMinutes();
      LOG.info(
          "Test ran for: {} minutes. Elements generated: {} | elements consumed: {}\n",
          testRuntime,
          inputElements,
          syndeoProcessed);
      if (0 < syndeoProcessed && 0 < inputElements && syndeoProcessed >= inputElements) {
        LOG.info("We are good to end!%n");
        syndeoResult.cancel();
        // HAPPY WE SUCCEEDED!
        break;
      }
      if (testRuntime > testConfig.getDurationMinutes() * 3
          || syndeoResult.getState().isTerminal()) {
        syndeoResult.cancel();
        fail(
            String.format(
                "Test ran for %s minutes. Pipeline only processed %s elements "
                    + "out of %s. Ending the test with failure.",
                testRuntime, syndeoProcessed, inputElements));
      }
      // Sleep 10 seconds before trying again.
      Thread.sleep(10_000L);
    }
  }
}
