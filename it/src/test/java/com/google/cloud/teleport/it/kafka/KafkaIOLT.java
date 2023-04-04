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
package com.google.cloud.teleport.it.kafka;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.it.IOLoadTestBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.synthetic.SyntheticOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticUnboundedSource;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaIO performance tests.
 *
 * <p>Example trigger command: "mvn test -pl it -am -Dtest="KafkaIOLT" -Dproject=[gcpProject] \
 * -DartifactBucket=[temp bucket] -DfailIfNoTests=false".
 */
public final class KafkaIOLT extends IOLoadTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaIOLT.class);

  private static KafkaResourceManager resourceManager;
  private static final String NAMESPACE = KafkaIOLT.class.getName();
  private static final String READ_ELEMENT_METRIC_NAME = "read_count";
  private static final int ROW_SIZE = 1024;
  private Configuration configuration;
  private String kafkaTopic;

  private SyntheticSourceOptions sourceOptions;

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() throws IOException {
    resourceManager = DefaultKafkaResourceManager.builder("io-kafka-lt").setHost(hostIp).build();
  }

  @Before
  public void setup() throws IOException {
    kafkaTopic =
        "io-kafka-"
            + DateTimeFormatter.ofPattern("MMddHHmmssSSS")
                .withZone(ZoneId.of("UTC"))
                .format(java.time.Instant.now())
            + UUID.randomUUID().toString().substring(0, 10);

    String testConfig =
        TestProperties.getProperty("configuration", "local", TestProperties.Type.PROPERTY);
    configuration = TEST_CONFIGS.get(testConfig);
    if (configuration == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown test configuration: [%s]. Known configs: %s",
              testConfig, TEST_CONFIGS.keySet()));
    }
    sourceOptions =
        SyntheticOptions.fromJsonString(
            configuration.getSourceOptions(), SyntheticSourceOptions.class);

    // tempLocation needs to be set for DataflowRunner
    if (!Strings.isNullOrEmpty(tempBucketName)) {
      String tempLocation = String.format("gs://%s/temp/", tempBucketName);
      writePipeline.getOptions().as(TestPipelineOptions.class).setTempRoot(tempLocation);
      writePipeline.getOptions().setTempLocation(tempLocation);
      readPipeline.getOptions().as(TestPipelineOptions.class).setTempRoot(tempLocation);
      readPipeline.getOptions().setTempLocation(tempLocation);
    }
    // Use streaming pipeline to write and read records
    writePipeline.getOptions().as(StreamingOptions.class).setStreaming(true);
    writePipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);
    readPipeline.getOptions().as(StreamingOptions.class).setStreaming(true);
    readPipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);
  }

  @AfterClass
  public static void tearDownClass() {
    ResourceManagerUtils.cleanResources(resourceManager);
  }

  private static final Map<String, Configuration> TEST_CONFIGS =
      ImmutableMap.of(
          "local", Configuration.of(1000L, 2, "DirectRunner"), // 1MB
          "medium", Configuration.of(10_000_000L, 20, "DataflowRunner"), // 10 GB
          "large", Configuration.of(100_000_000L, 80, "DataflowRunner") // 100 GB
          );

  /** Run integration test with configurations specified by TestProperties. */
  @Test
  public void testWriteAndRead() throws IOException {
    PipelineResult writeResult = testWrite();
    PipelineResult readResult = testRead();
    try {
      PipelineResult.State readState =
          readResult.waitUntilFinish(Duration.standardMinutes(configuration.getPipelineTimeout()));
      // as a streaming pipeline, null readState indicates pipeline is running instead of failed.
      assertNull(readState);
      // Fail the test if write pipeline (streaming) not in running state.
      PipelineResult.State writeState = writeResult.getState();
      assertNotEquals(PipelineResult.State.FAILED, writeState);
      long records = readElementMetric(readResult, NAMESPACE, READ_ELEMENT_METRIC_NAME);
      // TODO: there is a limitation (or bug) that the cache in KafkaWriter can stay indefinitely if
      // there is no upcoming records. Currently set expected records = (records generated - 10).
      assertTrue(
          String.format(
              "Actual number of record (%d) smaller than expected (at least %d).",
              records, sourceOptions.numRecords - 10),
          sourceOptions.numRecords - 10 <= records);
    } finally {
      // clean up pipelines
      if (writeResult.getState() == PipelineResult.State.RUNNING) {
        writeResult.cancel();
      }
      if (readResult.getState() == PipelineResult.State.RUNNING) {
        readResult.cancel();
      }
    }
  }

  private PipelineResult testWrite() throws IOException {

    KafkaIO.Write<byte[], byte[]> writeIO =
        KafkaIO.<byte[], byte[]>write()
            .withBootstrapServers(resourceManager.getBootstrapServers())
            .withTopic(kafkaTopic)
            .withKeySerializer(ByteArraySerializer.class)
            .withValueSerializer(ByteArraySerializer.class);

    writePipeline
        .apply(
            "Generate records",
            org.apache.beam.sdk.io.Read.from(new SyntheticUnboundedSource(sourceOptions)))
        .apply("Write to Kafka", writeIO.withTopic(kafkaTopic));

    return writePipeline.run();
  }

  private PipelineResult testRead() throws IOException {
    KafkaIO.Read<byte[], byte[]> readIO =
        KafkaIO.readBytes()
            .withBootstrapServers(resourceManager.getBootstrapServers())
            .withTopic(kafkaTopic)
            .withConsumerConfigUpdates(ImmutableMap.of("auto.offset.reset", "earliest"));
    readPipeline
        .apply("Read from unbounded Kafka", readIO)
        .apply("Counting element", ParDo.of(new CountingFn<>(NAMESPACE, READ_ELEMENT_METRIC_NAME)));

    return readPipeline.run();
  }

  private long readElementMetric(PipelineResult result, String namespace, String name) {
    MetricsReader metricsReader = new MetricsReader(result, namespace);
    return metricsReader.getCounterMetric(name);
  }

  /** Options for Kafka IO load test. */
  @AutoValue
  abstract static class Configuration {
    abstract Long getNumRows();

    abstract Integer getPipelineTimeout();

    abstract String getRunner();

    abstract Integer getRowSize();

    static Configuration of(long numRows, int pipelineTimeout, String runner) {
      return new AutoValue_KafkaIOLT_Configuration.Builder()
          .setNumRows(numRows)
          .setPipelineTimeout(pipelineTimeout)
          .setRunner(runner)
          .setRowSize(ROW_SIZE)
          .build();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setNumRows(long numRows);

      abstract Builder setPipelineTimeout(int timeOutMinutes);

      abstract Builder setRunner(String runner);

      abstract Builder setRowSize(int rowSize);

      abstract Configuration build();
    }

    abstract Builder toBuilder();

    /** Synthetic source options. */
    String getSourceOptions() {
      return String.format(
          "{\"numRecords\":%d,\"keySizeBytes\":4,\"valueSizeBytes\":%d}",
          getNumRows(), getRowSize());
    }
  }
}
