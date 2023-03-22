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
package com.google.cloud.teleport.it.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.it.IOLoadTestBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.synthetic.SyntheticBoundedSource;
import org.apache.beam.sdk.io.synthetic.SyntheticOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BigQueryIO performance tests.
 *
 * <p>Example trigger command: "mvn test -pl it -am -Dtest="BigQueryIOLT" -Dproject=[gcpProject] \
 * -DartifactBucket=[temp bucket] -DfailIfNoTests=false".
 */
public class BigQueryIOLT extends IOLoadTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIOLT.class);

  private static BigQueryResourceManager resourceManager;
  private static String tableQualifier;

  private static final String NAMESPACE = BigQueryIOLT.class.getName();
  private static final String READ_ELEMENT_METRIC_NAME = "read_count";
  private Configuration configuration;
  private String tempLocation;

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() {
    resourceManager =
        DefaultBigQueryResourceManager.builder("io-bigquery-lt", project)
            .setCredentials(CREDENTIALS)
            .build();
    resourceManager.createDataset(region);
  }

  @Before
  public void setup() throws IOException {
    String tableName =
        "io-bq-table-"
            + DateTimeFormatter.ofPattern("MMddHHmmssSSS")
                .withZone(ZoneId.of("UTC"))
                .format(java.time.Instant.now())
            + UUID.randomUUID().toString().substring(0, 10);
    tableQualifier = String.format("%s:%s.%s", project, resourceManager.getDatasetId(), tableName);

    String testConfig =
        TestProperties.getProperty("configuration", "local", TestProperties.Type.PROPERTY);
    configuration = TEST_CONFIGS.get(testConfig);
    if (configuration == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown test configuration: [%s]. Known configs: %s",
              testConfig, TEST_CONFIGS.keySet()));
    }
    // tempLocation needs to be set for bigquery IO writes
    if (!Strings.isNullOrEmpty(tempBucketName)) {
      tempLocation = String.format("gs://%s/", tempBucketName);
      readPipeline.getOptions().as(TestPipelineOptions.class).setTempRoot(tempLocation);
      readPipeline.getOptions().setTempLocation(tempLocation);
    }
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

  @Test
  public void testAvroFileLoadsWriteThenRead() throws IOException {
    configuration =
        configuration.toBuilder().setWriteFormat("AVRO").setWriteMethod("FILE_LOADS").build();
    testWriteAndRead();
  }

  @Test
  public void testJsonFileLoadsWriteThenRead() throws IOException {
    configuration =
        configuration.toBuilder().setWriteFormat("JSON").setWriteMethod("FILE_LOADS").build();
    testWriteAndRead();
  }

  @Test
  @Ignore("Avro streaming write is not supported as of Beam v2.45.0")
  public void testAvroStreamingWriteThenRead() throws IOException {
    configuration =
        configuration.toBuilder()
            .setWriteFormat("AVRO")
            .setWriteMethod("STREAMING_INSERTS")
            .build();
    testWriteAndRead();
  }

  @Test
  public void testJsonStreamingWriteThenRead() throws IOException {
    configuration =
        configuration.toBuilder()
            .setWriteFormat("JSON")
            .setWriteMethod("STREAMING_INSERTS")
            .build();
    testWriteAndRead();
  }

  /** Run integration test with configurations specified by TestProperties. */
  public void testWriteAndRead() throws IOException {
    WriteFormat writeFormat = WriteFormat.valueOf(configuration.getWriteFormat());
    BigQueryIO.Write<byte[]> writeIO = null;
    switch (writeFormat) {
      case AVRO:
        writeIO =
            BigQueryIO.<byte[]>write()
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withAvroFormatFunction(
                    writeRequest -> {
                      byte[] data = writeRequest.getElement();
                      GenericRecord record = new GenericData.Record(writeRequest.getSchema());
                      record.put("data", ByteBuffer.wrap(data));
                      return record;
                    });
        break;
      case JSON:
        writeIO =
            BigQueryIO.<byte[]>write()
                .withSuccessfulInsertsPropagation(false)
                .withFormatFunction(
                    input -> {
                      TableRow tableRow = new TableRow();
                      tableRow.set("data", Base64.getEncoder().encodeToString(input));
                      return tableRow;
                    });
        break;
    }
    testWrite(writeIO);
    testRead();
  }

  private void testWrite(BigQueryIO.Write<byte[]> writeIO) throws IOException {
    BigQueryIO.Write.Method method =
        BigQueryIO.Write.Method.valueOf(configuration.getWriteMethod());
    ArrayList<String> pipelineArgs = new ArrayList<String>();
    pipelineArgs.add("--runner=" + configuration.getRunner());
    if (method == BigQueryIO.Write.Method.STREAMING_INSERTS) {
      // set streaming for STREAMING_INSERTS write
      pipelineArgs.add("--streaming");
    }
    SyntheticSourceOptions sourceOption =
        SyntheticOptions.fromJsonString(
            configuration.getSourceOptions(), SyntheticSourceOptions.class);
    writePipeline
        .apply("Read from source", Read.from(new SyntheticBoundedSource(sourceOption)))
        .apply("Map records", ParDo.of(new MapKVToV()))
        .apply(
            "Write to BQ",
            writeIO
                .to(tableQualifier)
                .withMethod(method)
                .withSchema(
                    new TableSchema()
                        .setFields(
                            Collections.singletonList(
                                new TableFieldSchema().setName("data").setType("BYTES"))))
                .withCustomGcsTempLocation(ValueProvider.StaticValueProvider.of(tempLocation)));
    PipelineResult pipelineResult = writePipeline.runWithAdditionalOptionArgs(pipelineArgs);
    PipelineResult.State pipelineState =
        configuration.getPipelineTimeout() <= 0
            ? pipelineResult.waitUntilFinish()
            : pipelineResult.waitUntilFinish(
                Duration.standardMinutes(configuration.getPipelineTimeout()));
    // Fail the test if pipeline failed.
    assertNotEquals(PipelineResult.State.FAILED, pipelineState);
  }

  private void testRead() throws IOException {
    ArrayList<String> pipelineArgs = new ArrayList<String>();
    pipelineArgs.add("--runner=" + configuration.getRunner());
    if (!Strings.isNullOrEmpty(tempLocation)) {
      pipelineArgs.add("--tempLocation=" + tempLocation);
    }
    SyntheticSourceOptions sourceOption =
        SyntheticOptions.fromJsonString(
            configuration.getSourceOptions(), SyntheticSourceOptions.class);

    readPipeline
        .apply("Read from BQ", BigQueryIO.readTableRows().from(tableQualifier))
        .apply("Counting element", ParDo.of(new CountingFn<>(NAMESPACE, READ_ELEMENT_METRIC_NAME)));

    LOG.info("pipeline options: {}", readPipeline.getOptions());
    PipelineResult pipelineResult = readPipeline.run();
    PipelineResult.State pipelineState =
        configuration.getPipelineTimeout() <= 0
            ? pipelineResult.waitUntilFinish()
            : pipelineResult.waitUntilFinish(
                Duration.standardMinutes(configuration.getPipelineTimeout()));

    assertEquals(
        sourceOption.numRecords,
        readElementMetric(pipelineResult, NAMESPACE, READ_ELEMENT_METRIC_NAME));
    // Fail the test if pipeline failed.
    assertNotEquals(PipelineResult.State.FAILED, pipelineState);
  }

  private static class MapKVToV extends DoFn<KV<byte[], byte[]>, byte[]> {
    @ProcessElement
    public void process(ProcessContext context) {
      context.output(context.element().getValue());
    }
  }

  private static class CountingFn<T> extends DoFn<T, Void> {

    private final Counter elementCounter;

    CountingFn(String namespace, String name) {
      elementCounter = Metrics.counter(namespace, name);
    }

    @ProcessElement
    public void processElement() {
      elementCounter.inc(1L);
    }
  }

  private enum WriteFormat {
    AVRO,
    JSON
  }

  private long readElementMetric(PipelineResult result, String namespace, String name) {
    MetricsReader metricsReader = new MetricsReader(result, namespace);
    return metricsReader.getCounterMetric(name);
  }

  /** Options for Bigquery IO load test. */
  @AutoValue
  abstract static class Configuration {
    abstract Long getNumRows();

    abstract Integer getPipelineTimeout();

    abstract String getRunner();

    abstract Integer getRowSize();

    abstract String getWriteMethod();

    abstract String getWriteFormat();

    static Configuration of(long numRows, int pipelineTimeout, String runner) {
      return new AutoValue_BigQueryIOLT_Configuration.Builder()
          .setNumRows(numRows)
          .setPipelineTimeout(pipelineTimeout)
          .setRunner(runner)
          .setRowSize(1024)
          .setWriteMethod("DEFAULT")
          .setWriteFormat("AVRO")
          .build();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setNumRows(long numRows);

      abstract Builder setPipelineTimeout(int timeOutMinutes);

      abstract Builder setRunner(String runner);

      abstract Builder setRowSize(int rowSize);

      /**
       * Write method: DEFAULT/FILE_LOADS/STREAMING_INSERTS/STORAGE_WRITE_API(not yet supported).
       */
      abstract Builder setWriteMethod(String writeMethod);

      /** Write format: AVRO/JSON. */
      abstract Builder setWriteFormat(String writeFormat);

      abstract Configuration build();
    }

    abstract Builder toBuilder();

    /** Synthetic source options. */
    String getSourceOptions() {
      return String.format(
          "{\"numRecords\":%d,\"keySizeBytes\":1,\"valueSizeBytes\":%d}",
          getNumRows(), getRowSize());
    }
  }
}
