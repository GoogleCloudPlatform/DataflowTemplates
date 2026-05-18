/*
 * Copyright (C) 2025 Google LLC
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class DataStreamMongoDBToFirestoreTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final TupleTag<FailsafeElement<String, String>> SUCCESS_TAG =
      new TupleTag<FailsafeElement<String, String>>() {};

  @Test
  public void inputArgs_inputFilePattern() {
    String[] args = new String[] {"--inputFilePattern=gs://test-bkt/"};
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);
    String inputFilePattern = options.getInputFilePattern();

    assertEquals(inputFilePattern, "gs://test-bkt/");
  }

  @Test
  public void inputArgs_connectionUri_startWithMongodb() {
    String[] args = new String[] {"--connectionUri=mongodb://my-connection-string"};
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);
    String connectionUri = options.getConnectionUri();

    assertEquals(connectionUri, "mongodb://my-connection-string");
  }

  @Test
  public void inputArgs_connectionUri_startWithMongodbSrv() {
    String[] args = new String[] {"--connectionUri=mongodb+srv://my-connection-string"};
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);
    String connectionUri = options.getConnectionUri();

    assertEquals(connectionUri, "mongodb+srv://my-connection-string");
  }

  @Test
  public void inputArgs_connectionUri_invalid() {
    String[] args = new String[] {"--connectionUri=my-connection-string"};
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);

    assertThrows(IllegalArgumentException.class, () -> DataStreamMongoDBToFirestore.run(options));
  }

  @Test
  public void inputArgs_inputFileFormat_json() {
    String[] args = new String[] {"--inputFileFormat=json"};
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);
    String inputFileFormat = options.getInputFileFormat();

    assertEquals(inputFileFormat, "json");
  }

  @Test
  public void inputArgs_inputFileFormat_avro() {
    String[] args = new String[] {"--inputFileFormat=avro"};
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);
    String inputFileFormat = options.getInputFileFormat();

    assertEquals(inputFileFormat, "avro");
  }

  @Test
  public void inputArgs_inputFileFormat_invalid() {
    String[] args =
        new String[] {"--connectionUri=mongodb://my-connection-string", "--inputFileFormat=other"};
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);

    assertThrows(IllegalArgumentException.class, () -> DataStreamMongoDBToFirestore.run(options));
  }

  @Test
  public void inputArgs_javascriptTextTransformGcsPath() {
    String[] args = new String[] {"--javascriptTextTransformGcsPath=gs://test-bkt/udf.js"};
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);
    String path = options.getJavascriptTextTransformGcsPath();

    assertEquals(path, "gs://test-bkt/udf.js");
  }

  @Test
  public void inputArgs_javascriptTextTransformFunctionName() {
    String[] args = new String[] {"--javascriptTextTransformFunctionName=myTransform"};
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);
    String functionName = options.getJavascriptTextTransformFunctionName();

    assertEquals(functionName, "myTransform");
  }

  @Test
  public void prepareUdfInputFn_validJsonWithData() {
    String fullEventJson = "{\"data\":{\"name\":\"John\"}}";
    FailsafeElement<String, String> input = FailsafeElement.of(fullEventJson, fullEventJson);

    PCollectionTuple result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "PrepareUdfInput",
                ParDo.of(new DataStreamMongoDBToFirestore.PrepareUdfInputFn())
                    .withOutputTags(
                        SUCCESS_TAG,
                        TupleTagList.of(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG)));

    result
        .get(SUCCESS_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    result
        .get(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PAssert.that(result.get(SUCCESS_TAG))
        .satisfies(
            iterable -> {
              FailsafeElement<String, String> element = iterable.iterator().next();
              assertEquals(fullEventJson, element.getOriginalPayload());
              try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(element.getPayload());
                assertEquals("John", node.get("name").asText());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void prepareUdfInputFn_wrappedPayload() {
    // Test that PrepareUdfInputFn correctly extracts the inner event from a wrapped DLQ payload.
    // This simulates an event that has failed before and is being retried from the Dead Letter
    // Queue.
    // The structure contains the event under "changeEvent" and metadata like "dataCollection".
    String fullEventJson =
        "{\"changeEvent\":{\"data\":\"{\\\"name\\\":\\\"John\\\"}\"},\"dataCollection\":\"test\"}";

    // We pass the fullEventJson as both payload and originalPayload, which is typical for the start
    // of a retry flow.
    FailsafeElement<String, String> input = FailsafeElement.of(fullEventJson, fullEventJson);

    PCollectionTuple result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "PrepareUdfInput",
                ParDo.of(new DataStreamMongoDBToFirestore.PrepareUdfInputFn())
                    .withOutputTags(
                        SUCCESS_TAG,
                        TupleTagList.of(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG)));

    result
        .get(SUCCESS_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    result
        .get(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PAssert.that(result.get(SUCCESS_TAG))
        .satisfies(
            iterable -> {
              FailsafeElement<String, String> element = iterable.iterator().next();
              // Verify that the original payload is preserved intact for DLQ purposes.
              assertEquals(fullEventJson, element.getOriginalPayload());
              try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(element.getPayload());
                // Verify that PrepareUdfInputFn successfully extracted the inner "data" field
                // from the wrapped "changeEvent", rather than passing the whole wrapper to UDF.
                assertEquals("John", node.get("name").asText());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void prepareUdfInputFn_missingData_routesToDlq() {
    String fullEventJson = "{\"op\":\"u\"}";
    FailsafeElement<String, String> input = FailsafeElement.of(fullEventJson, fullEventJson);

    PCollectionTuple result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "PrepareUdfInput",
                ParDo.of(new DataStreamMongoDBToFirestore.PrepareUdfInputFn())
                    .withOutputTags(
                        SUCCESS_TAG,
                        TupleTagList.of(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG)));

    result
        .get(SUCCESS_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    result
        .get(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PAssert.that(result.get(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG))
        .satisfies(
            iterable -> {
              FailsafeElement<String, String> element = iterable.iterator().next();
              assertEquals(fullEventJson, element.getOriginalPayload());
              assertEquals(fullEventJson, element.getPayload());
              assertEquals(
                  "Missing data field in event or unsupported data field type",
                  element.getErrorMessage());
              return null;
            });

    pipeline.run();
  }

  @Test
  public void prepareUdfInputFn_updateWithNullData_skipsEvent() {
    String fullEventJson = "{\"_metadata_change_type\":\"UPDATE\"}";
    FailsafeElement<String, String> input = FailsafeElement.of(fullEventJson, fullEventJson);

    PCollectionTuple result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "PrepareUdfInput",
                ParDo.of(new DataStreamMongoDBToFirestore.PrepareUdfInputFn())
                    .withOutputTags(
                        SUCCESS_TAG,
                        TupleTagList.of(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG)));

    result
        .get(SUCCESS_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    result
        .get(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PAssert.that(result.get(SUCCESS_TAG)).empty();
    PAssert.that(result.get(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG)).empty();

    PipelineResult pipelineResult = pipeline.run();

    MetricQueryResults metrics =
        pipelineResult
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            DataStreamMongoDBToFirestore.PrepareUdfInputFn.class,
                            "skippedUpdatesWithNullData"))
                    .build());

    long count = 0;
    if (metrics.getCounters().iterator().hasNext()) {
      count = metrics.getCounters().iterator().next().getAttempted();
    }
    assertEquals(1L, count);
  }

  @Test
  public void prepareUdfInputFn_insertWithNullData_routesToDlq() {
    String fullEventJson = "{\"_metadata_change_type\":\"INSERT\"}";
    FailsafeElement<String, String> input = FailsafeElement.of(fullEventJson, fullEventJson);

    PCollectionTuple result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "PrepareUdfInput",
                ParDo.of(new DataStreamMongoDBToFirestore.PrepareUdfInputFn())
                    .withOutputTags(
                        SUCCESS_TAG,
                        TupleTagList.of(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG)));

    result
        .get(SUCCESS_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    result
        .get(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PAssert.that(result.get(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG))
        .satisfies(
            iterable -> {
              FailsafeElement<String, String> element = iterable.iterator().next();
              assertEquals(fullEventJson, element.getOriginalPayload());
              assertEquals(
                  "Missing data field in event or unsupported data field type",
                  element.getErrorMessage());
              return null;
            });

    pipeline.run();
  }

  @Test
  public void prepareUdfInputFn_wrappedEvent_succeeds() {
    String fullEventJson =
        "{\"changeEvent\":{\"_metadata_change_type\":\"UPDATE\",\"data\":{\"field\":\"value\"}}}";
    FailsafeElement<String, String> input = FailsafeElement.of(fullEventJson, fullEventJson);

    PCollectionTuple result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "PrepareUdfInput",
                ParDo.of(new DataStreamMongoDBToFirestore.PrepareUdfInputFn())
                    .withOutputTags(
                        SUCCESS_TAG,
                        TupleTagList.of(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG)));

    result
        .get(SUCCESS_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    result
        .get(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PAssert.that(result.get(SUCCESS_TAG))
        .satisfies(
            iterable -> {
              FailsafeElement<String, String> element = iterable.iterator().next();
              assertEquals(fullEventJson, element.getOriginalPayload());
              assertTrue(element.getPayload().contains("\"field\": \"value\""));
              return null;
            });

    pipeline.run();
  }

  @Test
  public void prepareUdfInputFn_invalidJson_routesToDlq() {
    String invalidJson = "{invalid}";
    FailsafeElement<String, String> input = FailsafeElement.of(invalidJson, invalidJson);

    PCollectionTuple result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "PrepareUdfInput",
                ParDo.of(new DataStreamMongoDBToFirestore.PrepareUdfInputFn())
                    .withOutputTags(
                        SUCCESS_TAG,
                        TupleTagList.of(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG)));

    result
        .get(SUCCESS_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    result
        .get(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PAssert.that(result.get(DataStreamMongoDBToFirestore.PREPARE_FAILURE_TAG))
        .satisfies(
            iterable -> {
              FailsafeElement<String, String> element = iterable.iterator().next();
              assertEquals(invalidJson, element.getOriginalPayload());
              assertEquals(invalidJson, element.getPayload());
              org.junit.Assert.assertNotNull(element.getErrorMessage());
              return null;
            });

    pipeline.run();
  }

  @Test
  public void restoreUdfOutputFn_unchangedPayload() {
    String fullEventJson = "{\"data\":\"{\\\"name\\\":\\\"John\\\"}\"}";
    String canonicalJson = "{\"name\":\"John\"}";
    FailsafeElement<String, String> input = FailsafeElement.of(fullEventJson, canonicalJson);

    PCollectionTuple result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "RestoreUdfOutput",
                ParDo.of(new DataStreamMongoDBToFirestore.RestoreUdfOutputFn())
                    .withOutputTags(
                        SUCCESS_TAG,
                        TupleTagList.of(DataStreamMongoDBToFirestore.RESTORE_FAILURE_TAG)));

    result
        .get(SUCCESS_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    result
        .get(DataStreamMongoDBToFirestore.RESTORE_FAILURE_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PAssert.that(result.get(SUCCESS_TAG))
        .satisfies(
            iterable -> {
              FailsafeElement<String, String> element = iterable.iterator().next();
              try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(element.getPayload());
                assertEquals("{\"name\":\"John\"}", node.get("data").asText());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void restoreUdfOutputFn_invalidTransformedData_routesToDlq() {
    String fullEventJson = "{\"data\":\"{\\\"name\\\":\\\"John\\\"}\"}";
    String invalidTransformedData = "invalid json";
    FailsafeElement<String, String> input =
        FailsafeElement.of(fullEventJson, invalidTransformedData);

    PCollectionTuple result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "RestoreUdfOutput",
                ParDo.of(new DataStreamMongoDBToFirestore.RestoreUdfOutputFn())
                    .withOutputTags(
                        SUCCESS_TAG,
                        TupleTagList.of(DataStreamMongoDBToFirestore.RESTORE_FAILURE_TAG)));

    result
        .get(SUCCESS_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    result
        .get(DataStreamMongoDBToFirestore.RESTORE_FAILURE_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PAssert.that(result.get(DataStreamMongoDBToFirestore.RESTORE_FAILURE_TAG))
        .satisfies(
            iterable -> {
              FailsafeElement<String, String> element = iterable.iterator().next();
              assertEquals(fullEventJson, element.getOriginalPayload());
              assertEquals(invalidTransformedData, element.getPayload());
              assertNotNull(element.getErrorMessage());
              return null;
            });

    pipeline.run();
  }

  @Test
  public void restoreUdfOutputFn_changedPayload() {
    // Test that RestoreUdfOutputFn correctly merges transformed data back into the full event
    // and overwrites BOTH originalPayload and payload with the modified event JSON.
    String fullEventJson = "{\"data\":{\"name\":\"John\"}}";
    String transformedData = "{\"name\":\"Jane\"}";
    FailsafeElement<String, String> input = FailsafeElement.of(fullEventJson, transformedData);

    PCollectionTuple result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "RestoreUdfOutput",
                ParDo.of(new DataStreamMongoDBToFirestore.RestoreUdfOutputFn())
                    .withOutputTags(
                        SUCCESS_TAG,
                        TupleTagList.of(DataStreamMongoDBToFirestore.RESTORE_FAILURE_TAG)));

    result
        .get(SUCCESS_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    result
        .get(DataStreamMongoDBToFirestore.RESTORE_FAILURE_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PAssert.that(result.get(SUCCESS_TAG))
        .satisfies(
            iterable -> {
              FailsafeElement<String, String> element = iterable.iterator().next();
              try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(element.getPayload());
                String dataStr = node.get("data").asText();
                JsonNode dataNode = mapper.readTree(dataStr);
                assertEquals("Jane", dataNode.get("name").asText());

                // Verify that originalPayload is PRESERVED and not overwritten
                JsonNode originalNode = mapper.readTree(element.getOriginalPayload());
                JsonNode originalDataNode = originalNode.get("data");
                assertEquals("John", originalDataNode.get("name").asText());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void restoreUdfOutputFn_wrappedPayload() {
    // Test that RestoreUdfOutputFn correctly merges transformed data back into the wrapped event
    // (DLQ format). This ensures that when we retry an event from DLQ and apply UDF, the result
    // is correctly placed back into the wrapped structure for further processing.
    String fullEventJson =
        "{\"changeEvent\":{\"data\":\"{\\\"name\\\":\\\"John\\\"}\"},\"dataCollection\":\"test\"}";
    String transformedData = "{\"name\":\"Jane\"}";
    FailsafeElement<String, String> input = FailsafeElement.of(fullEventJson, transformedData);

    PCollectionTuple result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "RestoreUdfOutput",
                ParDo.of(new DataStreamMongoDBToFirestore.RestoreUdfOutputFn())
                    .withOutputTags(
                        SUCCESS_TAG,
                        TupleTagList.of(DataStreamMongoDBToFirestore.RESTORE_FAILURE_TAG)));

    result
        .get(SUCCESS_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    result
        .get(DataStreamMongoDBToFirestore.RESTORE_FAILURE_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PAssert.that(result.get(SUCCESS_TAG))
        .satisfies(
            iterable -> {
              FailsafeElement<String, String> element = iterable.iterator().next();
              try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(element.getPayload());

                // Verify that the inner changeEvent has the updated data after UDF application.
                JsonNode innerEvent = node.get("changeEvent");
                String dataStr = innerEvent.get("data").asText();
                JsonNode dataNode = mapper.readTree(dataStr);
                assertEquals("Jane", dataNode.get("name").asText());

                // Verify that originalPayload is PRESERVED as the original wrapped event,
                // ensuring auditability and safety for further retries.
                JsonNode originalNode = mapper.readTree(element.getOriginalPayload());
                JsonNode originalInnerEvent = originalNode.get("changeEvent");
                String originalDataStr = originalInnerEvent.get("data").asText();
                JsonNode originalDataNode = mapper.readTree(originalDataStr);
                assertEquals("John", originalDataNode.get("name").asText());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void applyUdfToDataField_fullTransform() throws IOException {
    String udfCode =
        "function transform(inJson) {\n"
            + "  var obj = JSON.parse(inJson);\n"
            + "  obj.name = obj.name.toUpperCase();\n"
            + "  return JSON.stringify(obj);\n"
            + "}";
    Path udfFile = Files.createTempFile("test-udf", ".js");
    Files.write(udfFile, udfCode.getBytes());

    String[] args =
        new String[] {
          "--javascriptTextTransformGcsPath=" + udfFile.toAbsolutePath().toString(),
          "--javascriptTextTransformFunctionName=transform",
          "--deadLetterQueueDirectory=/tmp/dlq"
        };
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);

    DeadLetterQueueManager dlqManager = DeadLetterQueueManager.create("/tmp/dlq", 3);

    String fullEventJson = "{\"data\":{\"name\":\"john\"}}";
    FailsafeElement<String, String> input = FailsafeElement.of(fullEventJson, fullEventJson);

    PCollection<FailsafeElement<String, String>> result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "ApplyUdf",
                new DataStreamMongoDBToFirestore.ApplyUdfToDataField(options, dlqManager));

    PAssert.that(result)
        .satisfies(
            iterable -> {
              FailsafeElement<String, String> element = iterable.iterator().next();
              try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(element.getPayload());
                String dataStr = node.get("data").asText();
                JsonNode dataNode = mapper.readTree(dataStr);
                assertEquals("JOHN", dataNode.get("name").asText());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return null;
            });

    pipeline.run();

    Files.delete(udfFile);
  }

  @Test
  public void applyUdfToDataField_specialValues() throws IOException {
    String udfCode =
        "function transform(inJson) {\n"
            + "  var obj = JSON.parse(inJson);\n"
            + "  obj.processed = true;\n"
            + "  return JSON.stringify(obj);\n"
            + "}";
    Path udfFile = Files.createTempFile("test-udf-special", ".js");
    Files.write(udfFile, udfCode.getBytes());

    String[] args =
        new String[] {
          "--javascriptTextTransformGcsPath=" + udfFile.toAbsolutePath().toString(),
          "--javascriptTextTransformFunctionName=transform",
          "--deadLetterQueueDirectory=/tmp/dlq"
        };
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);

    DeadLetterQueueManager dlqManager = DeadLetterQueueManager.create("/tmp/dlq", 3);

    String fullEventJson =
        "{\"data\":{\"_id\":{\"$oid\":\"507f1f77bcf86cd799439011\"},\"nan\":{\"$numberDouble\":\"NaN\"},\"inf\":{\"$numberDouble\":\"Infinity\"},\"negInf\":{\"$numberDouble\":\"-Infinity\"},\"dbl\":3.14,\"lng\":{\"$numberLong\":\"1234567890\"},\"ts\":{\"$timestamp\":{\"t\":123,\"i\":456}}}}";
    FailsafeElement<String, String> input = FailsafeElement.of(fullEventJson, fullEventJson);

    PCollection<FailsafeElement<String, String>> result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "ApplyUdf",
                new DataStreamMongoDBToFirestore.ApplyUdfToDataField(options, dlqManager));

    PAssert.that(result)
        .satisfies(
            iterable -> {
              FailsafeElement<String, String> element = iterable.iterator().next();
              try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(element.getPayload());
                String dataStr = node.get("data").asText();

                org.bson.Document doc = org.bson.Document.parse(dataStr);

                assertEquals(Double.NaN, doc.getDouble("nan"), 0.0);
                assertEquals(Double.POSITIVE_INFINITY, doc.getDouble("inf"), 0.0);
                assertEquals(Double.NEGATIVE_INFINITY, doc.getDouble("negInf"), 0.0);
                assertEquals(3.14, doc.getDouble("dbl"), 0.0);
                assertEquals(1234567890L, doc.getLong("lng").longValue());
                assertEquals(
                    new org.bson.types.ObjectId("507f1f77bcf86cd799439011"),
                    doc.getObjectId("_id"));
                assertEquals(
                    new org.bson.BsonTimestamp(123, 456),
                    doc.get("ts", org.bson.BsonTimestamp.class));
                assertEquals(true, doc.getBoolean("processed"));

              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return null;
            });

    pipeline.run();

    Files.delete(udfFile);
  }

  @Test
  public void applyUdfToDataField_updateWithNullData_skipsEvent() throws IOException {
    String udfCode =
        "function transform(inJson) {\n"
            + "  var obj = JSON.parse(inJson);\n"
            + "  obj.processed = true;\n"
            + "  return JSON.stringify(obj);\n"
            + "}";
    Path udfFile = Files.createTempFile("test-udf-skip", ".js");
    Files.write(udfFile, udfCode.getBytes());

    String[] args =
        new String[] {
          "--javascriptTextTransformGcsPath=" + udfFile.toAbsolutePath().toString(),
          "--javascriptTextTransformFunctionName=transform",
          "--deadLetterQueueDirectory=/tmp/dlq"
        };
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);

    DeadLetterQueueManager dlqManager = DeadLetterQueueManager.create("/tmp/dlq", 3);

    String fullEventJson = "{\"_metadata_change_type\":\"UPDATE\"}";
    FailsafeElement<String, String> input = FailsafeElement.of(fullEventJson, fullEventJson);

    PCollection<FailsafeElement<String, String>> result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "ApplyUdf",
                new DataStreamMongoDBToFirestore.ApplyUdfToDataField(options, dlqManager));

    PAssert.that(result).empty();

    pipeline.run();

    Files.delete(udfFile);
  }

  @Test
  public void applyUdfToDataField_udfFails_emitsEmptyResult() throws IOException {
    String udfCode =
        "function transform(inJson) {\n" + "  throw new Error(\"UDF failed\");\n" + "}";
    Path udfFile = Files.createTempFile("test-udf-fail", ".js");
    Files.write(udfFile, udfCode.getBytes());

    String[] args =
        new String[] {
          "--javascriptTextTransformGcsPath=" + udfFile.toAbsolutePath().toString(),
          "--javascriptTextTransformFunctionName=transform",
          "--deadLetterQueueDirectory=/tmp/dlq"
        };
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);

    DeadLetterQueueManager dlqManager = DeadLetterQueueManager.create("/tmp/dlq", 3);

    String fullEventJson = "{\"data\":{\"name\":\"john\"}}";
    FailsafeElement<String, String> input = FailsafeElement.of(fullEventJson, fullEventJson);

    PCollection<FailsafeElement<String, String>> result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "ApplyUdf",
                new DataStreamMongoDBToFirestore.ApplyUdfToDataField(options, dlqManager));

    // The result should be empty because the event failed UDF and went to DLQ
    PAssert.that(result).empty();

    pipeline.run();

    Files.delete(udfFile);
  }

  @Test
  public void applyUdfToDataField_udfReturnsNonBson_emitsEmptyResult() throws IOException {
    String udfCode = "function transform(inJson) {\n" + "  return \"invalid json\";\n" + "}";
    Path udfFile = Files.createTempFile("test-udf-non-bson", ".js");
    Files.write(udfFile, udfCode.getBytes());

    String[] args =
        new String[] {
          "--javascriptTextTransformGcsPath=" + udfFile.toAbsolutePath().toString(),
          "--javascriptTextTransformFunctionName=transform",
          "--deadLetterQueueDirectory=/tmp/dlq"
        };
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);

    DeadLetterQueueManager dlqManager = DeadLetterQueueManager.create("/tmp/dlq", 3);

    String fullEventJson = "{\"data\":{\"name\":\"john\"}}";
    FailsafeElement<String, String> input = FailsafeElement.of(fullEventJson, fullEventJson);

    PCollection<FailsafeElement<String, String>> result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "ApplyUdf",
                new DataStreamMongoDBToFirestore.ApplyUdfToDataField(options, dlqManager));

    // If we expect it to be DLQed, the successful result should be empty!
    PAssert.that(result).empty();

    pipeline.run();

    Files.delete(udfFile);
  }

  @Test
  public void applyUdfToDataField_deleteEvent_bypassesUdf() throws IOException {
    String udfCode =
        "function transform(inJson) {\n"
            + "  var obj = JSON.parse(inJson);\n"
            + "  obj.processed = true;\n"
            + "  return JSON.stringify(obj);\n"
            + "}";
    Path udfFile = Files.createTempFile("test-udf-delete", ".js");
    Files.write(udfFile, udfCode.getBytes());

    String[] args =
        new String[] {
          "--javascriptTextTransformGcsPath=" + udfFile.toAbsolutePath().toString(),
          "--javascriptTextTransformFunctionName=transform",
          "--deadLetterQueueDirectory=/tmp/dlq"
        };
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);

    DeadLetterQueueManager dlqManager = DeadLetterQueueManager.create("/tmp/dlq", 3);

    String fullEventJson = "{\"_metadata_change_type\":\"DELETE\",\"data\":{\"name\":\"John\"}}";
    FailsafeElement<String, String> input = FailsafeElement.of(fullEventJson, fullEventJson);

    PCollection<FailsafeElement<String, String>> result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "ApplyUdf",
                new DataStreamMongoDBToFirestore.ApplyUdfToDataField(options, dlqManager));

    PAssert.that(result)
        .satisfies(
            iterable -> {
              FailsafeElement<String, String> element = iterable.iterator().next();
              try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(element.getPayload());
                assertEquals("DELETE", node.get("_metadata_change_type").asText());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return null;
            });

    pipeline.run();

    Files.delete(udfFile);
  }

  @Test
  public void applyUdfToDataField_mixedEvents_correctlyProcessed() throws IOException {
    String udfCode =
        "function transform(inJson) {\n"
            + "  var obj = JSON.parse(inJson);\n"
            + "  obj.processed = true;\n"
            + "  return JSON.stringify(obj);\n"
            + "}";
    Path udfFile = Files.createTempFile("test-udf-mixed", ".js");
    Files.write(udfFile, udfCode.getBytes());

    String[] args =
        new String[] {
          "--javascriptTextTransformGcsPath=" + udfFile.toAbsolutePath().toString(),
          "--javascriptTextTransformFunctionName=transform",
          "--deadLetterQueueDirectory=/tmp/dlq"
        };
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);

    DeadLetterQueueManager dlqManager = DeadLetterQueueManager.create("/tmp/dlq", 3);

    String insertJson = "{\"_metadata_change_type\":\"INSERT\",\"data\":{\"name\":\"inv1\"}}";
    String deleteJson = "{\"_metadata_change_type\":\"DELETE\",\"data\":{\"name\":\"del1\"}}";
    String updateWithDataJson =
        "{\"_metadata_change_type\":\"UPDATE\",\"data\":{\"name\":\"upd1\"}}";
    String updateNullDataJson = "{\"_metadata_change_type\":\"UPDATE\"}";
    String readJson = "{\"_metadata_change_type\":\"READ\",\"data\":{\"name\":\"rd1\"}}";

    java.util.List<FailsafeElement<String, String>> inputs =
        java.util.Arrays.asList(
            FailsafeElement.of(insertJson, insertJson),
            FailsafeElement.of(deleteJson, deleteJson),
            FailsafeElement.of(updateWithDataJson, updateWithDataJson),
            FailsafeElement.of(updateNullDataJson, updateNullDataJson),
            FailsafeElement.of(readJson, readJson));

    PCollection<FailsafeElement<String, String>> result =
        pipeline
            .apply(
                "CreateInput",
                Create.of(inputs)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                "ApplyUdf",
                new DataStreamMongoDBToFirestore.ApplyUdfToDataField(options, dlqManager));

    PAssert.that(result)
        .satisfies(
            iterable -> {
              java.util.List<FailsafeElement<String, String>> elements =
                  new java.util.ArrayList<>();
              iterable.forEach(elements::add);

              assertEquals(4, elements.size());

              java.util.Map<String, JsonNode> eventMap = new java.util.HashMap<>();
              ObjectMapper mapper = new ObjectMapper();

              for (FailsafeElement<String, String> element : elements) {
                try {
                  JsonNode node = mapper.readTree(element.getPayload());
                  String type = node.get("_metadata_change_type").asText();
                  eventMap.put(type, node);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }

              assertTrue(eventMap.containsKey("INSERT"));
              assertTrue(eventMap.containsKey("DELETE"));
              assertTrue(eventMap.containsKey("UPDATE"));
              assertTrue(eventMap.containsKey("READ"));

              try {
                // Verify UDF was applied to INSERT, UPDATE, READ
                String insertDataStr = eventMap.get("INSERT").get("data").asText();
                JsonNode insertDataNode = mapper.readTree(insertDataStr);
                assertTrue(insertDataNode.has("processed"));

                String updateDataStr = eventMap.get("UPDATE").get("data").asText();
                JsonNode updateDataNode = mapper.readTree(updateDataStr);
                assertTrue(updateDataNode.has("processed"));

                String readDataStr = eventMap.get("READ").get("data").asText();
                JsonNode readDataNode = mapper.readTree(readDataStr);
                assertTrue(readDataNode.has("processed"));

                // Verify UDF was NOT applied to DELETE
                JsonNode deleteDataNode = eventMap.get("DELETE").get("data");
                assertFalse(deleteDataNode.has("processed"));

              } catch (Exception e) {
                throw new RuntimeException(e);
              }

              return null;
            });

    pipeline.run();

    Files.delete(udfFile);
  }
}
