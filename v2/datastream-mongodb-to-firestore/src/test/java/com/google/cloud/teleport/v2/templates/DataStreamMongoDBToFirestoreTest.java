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
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.beam.sdk.coders.StringUtf8Coder;
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

    FailsafeElement<String, String> expectedOutput =
        FailsafeElement.of(fullEventJson, fullEventJson);
    PAssert.that(result.get(SUCCESS_TAG)).containsInAnyOrder(expectedOutput);

    pipeline.run();
  }

  @Test
  public void restoreUdfOutputFn_changedPayload() {
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
        "{\"data\":{\"nan\":{\"$numberDouble\":\"NaN\"},\"inf\":{\"$numberDouble\":\"Infinity\"},\"negInf\":{\"$numberDouble\":\"-Infinity\"},\"dbl\":3.14,\"lng\":{\"$numberLong\":\"1234567890\"}}}";
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

              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return null;
            });

    pipeline.run();

    Files.delete(udfFile);
  }
}
