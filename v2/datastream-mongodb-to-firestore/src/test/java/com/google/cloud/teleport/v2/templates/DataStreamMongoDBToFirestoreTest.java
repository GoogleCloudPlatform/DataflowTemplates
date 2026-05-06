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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import com.google.cloud.teleport.v2.transforms.CreateMongoDbChangeEventContextFn;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.util.List;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
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

  private static final TupleTag<MongoDbChangeEventContext> PARSE_SUCCESS_TAG = new TupleTag<MongoDbChangeEventContext>() {};
  private static final TupleTag<FailsafeElement<MongoDbChangeEventContext, String>> PARSE_FAILURE_TAG = new TupleTag<FailsafeElement<MongoDbChangeEventContext, String>>() {};
  private static final TupleTag<FailsafeElement<MongoDbChangeEventContext, String>> UDF_EXECUTION_FAILURE_TAG = new TupleTag<FailsafeElement<MongoDbChangeEventContext, String>>() {};
  private static final java.util.List<MongoDbChangeEventContext> TEST_OUTPUT = new java.util.ArrayList<>();

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
  public void testPipeline_udfModifiesData() throws Exception {
    org.apache.beam.sdk.coders.CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    com.google.cloud.teleport.v2.coders.FailsafeElementCoder<String, String> failsafeCoder = com.google.cloud.teleport.v2.coders.FailsafeElementCoder.of(org.apache.beam.sdk.coders.StringUtf8Coder.of(), org.apache.beam.sdk.coders.StringUtf8Coder.of());
    coderRegistry.registerCoderForType(
        failsafeCoder.getEncodedTypeDescriptor(),
        failsafeCoder);
        
    TEST_OUTPUT.clear();

    // 1. Create temp JS file
    java.nio.file.Path tempJs = java.nio.file.Files.createTempFile("transform", ".js");
    java.nio.file.Files.writeString(tempJs, 
        "function myTransform(inJson) {\n" +
        "  var obj = JSON.parse(inJson);\n" +
        "  obj.transformed = true;\n" +
        "  return JSON.stringify(obj);\n" +
        "}");
        
    String insertJson = "{\"_metadata_source\":{\"collection\":\"test_collection\"},\"_id\":\"\\\"645c9a7e7b8b1a0e9c0f8b3a\\\"\",\"data\":{\"myDate\":{\"$date\":\"2023-05-11T15:00:00Z\"},\"myNaN\":{\"$numberDouble\":\"NaN\"},\"myInf\":{\"$numberDouble\":\"Infinity\"}},\"_metadata_timestamp_seconds\":1683782270,\"_metadata_timestamp_nanos\":123456789}";
    
    FailsafeElement<String, String> failsafeElement = FailsafeElement.of(insertJson, insertJson);
    PCollection<FailsafeElement<String, String>> input = pipeline.apply(
        Create.of(java.util.Arrays.asList(failsafeElement))
            .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));
            
    PCollectionTuple contexts = input.apply(
        "Create Contexts",
        ParDo.of(new CreateMongoDbChangeEventContextFn("shadow_"))
            .withOutputTags(
                CreateMongoDbChangeEventContextFn.successfulCreationTag,
                TupleTagList.of(CreateMongoDbChangeEventContextFn.failedCreationTag)));
    contexts.get(CreateMongoDbChangeEventContextFn.failedCreationTag).setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
                
    PCollection<MongoDbChangeEventContext> successfulContexts = contexts.get(CreateMongoDbChangeEventContextFn.successfulCreationTag);
    
    PCollectionTuple udfPreparation = successfulContexts.apply(
        "Prepare UDF Input",
        ParDo.of(new DataStreamMongoDBToFirestore.ExtractUdfInputFn())
            .withOutputTags(
                DataStreamMongoDBToFirestore.ExtractUdfInputFn.UDF_INPUT_TAG,
                TupleTagList.of(DataStreamMongoDBToFirestore.ExtractUdfInputFn.DELETES_TAG)));
                
    PCollection<FailsafeElement<MongoDbChangeEventContext, String>> udfInput = udfPreparation.get(DataStreamMongoDBToFirestore.ExtractUdfInputFn.UDF_INPUT_TAG);
    udfInput.setCoder(FailsafeElementCoder.of(SerializableCoder.of(MongoDbChangeEventContext.class), StringUtf8Coder.of()));
    
    PCollection<FailsafeElement<MongoDbChangeEventContext, String>> udfSuccess = udfInput.apply(
        "Simulate Modifying UDF",
        ParDo.of(new SimulateModifyingUdfFn()));
        
    org.apache.beam.sdk.values.PCollectionView<FailsafeElement<MongoDbChangeEventContext, String>> udfView = 
        udfSuccess.apply(org.apache.beam.sdk.transforms.View.asSingleton());
        
    PCollection<MongoDbChangeEventContext> output = successfulContexts.apply(
        "Merge UDF Result",
        ParDo.of(new MergeWithSideInputFn(udfView, "shadow_"))
            .withSideInputs(udfView));
        
    pipeline.run();
    
    assertEquals(1, TEST_OUTPUT.size());
    MongoDbChangeEventContext ctx = TEST_OUTPUT.get(0);
    String modifiedJson = ctx.getModifiedJsonStringData();
    System.out.println("Test assertion: modifiedJson=" + modifiedJson);
    org.junit.Assert.assertTrue(modifiedJson.contains("\"transformed\": true"));
    org.junit.Assert.assertTrue(modifiedJson.contains("\"$date\""));
    org.junit.Assert.assertTrue(modifiedJson.contains("\"$numberDouble\": \"NaN\""));
    org.junit.Assert.assertTrue(modifiedJson.contains("\"$numberDouble\": \"Infinity\""));
    
    java.nio.file.Files.delete(tempJs);
  }

  @Test
  public void testPipeline_udfInvalidJsonOutput() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String insertJson = "{\"_metadata_source\":{\"collection\":\"col\"},\"_id\":\"\\\"id1\\\"\",\"data\":{\"field\":\"val\"},\"_metadata_timestamp_seconds\":123,\"_metadata_timestamp_nanos\":456,\"op\":\"i\"}";
    MongoDbChangeEventContext event = new MongoDbChangeEventContext(mapper.readTree(insertJson), "shadow_");
    
    List<MongoDbChangeEventContext> events = java.util.Arrays.asList(event);
    PCollection<MongoDbChangeEventContext> input = pipeline.apply(Create.of(events));
    
    PCollectionTuple udfPreparation = input.apply(
        "Prepare UDF Input",
        ParDo.of(new DataStreamMongoDBToFirestore.ExtractUdfInputFn())
            .withOutputTags(
                DataStreamMongoDBToFirestore.ExtractUdfInputFn.UDF_INPUT_TAG,
                TupleTagList.of(DataStreamMongoDBToFirestore.ExtractUdfInputFn.DELETES_TAG)));
                
    PCollection<FailsafeElement<MongoDbChangeEventContext, String>> udfInput = udfPreparation.get(DataStreamMongoDBToFirestore.ExtractUdfInputFn.UDF_INPUT_TAG);
    udfInput.setCoder(FailsafeElementCoder.of(SerializableCoder.of(MongoDbChangeEventContext.class), StringUtf8Coder.of()));
    
    PCollection<FailsafeElement<MongoDbChangeEventContext, String>> udfSuccess = udfInput.apply(
        "Simulate Invalid UDF Output",
        ParDo.of(new SimulateInvalidUdfOutputFn()));
        
    PCollectionTuple mergeResult = udfSuccess.apply(
        "Merge UDF Result",
        ParDo.of(new DataStreamMongoDBToFirestore.MergeUdfResultFn(PARSE_FAILURE_TAG, "shadow_"))
            .withOutputTags(PARSE_SUCCESS_TAG, TupleTagList.of(PARSE_FAILURE_TAG)));
    mergeResult.get(PARSE_FAILURE_TAG).setCoder(FailsafeElementCoder.of(SerializableCoder.of(MongoDbChangeEventContext.class), StringUtf8Coder.of()));
            
    PAssert.that(mergeResult.get(PARSE_FAILURE_TAG))
        .satisfies(
            collection -> {
              FailsafeElement<MongoDbChangeEventContext, String> result = collection.iterator().next();
              assertEquals("invalid json", result.getPayload());
              return null;
            });
            
    pipeline.run();
  }

  @Test
  public void testPipeline_udfExecutionFailure() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String insertJson = "{\"_metadata_source\":{\"collection\":\"col\"},\"_id\":\"\\\"id1\\\"\",\"data\":{\"field\":\"val\"},\"_metadata_timestamp_seconds\":123,\"_metadata_timestamp_nanos\":456,\"op\":\"i\"}";
    MongoDbChangeEventContext event = new MongoDbChangeEventContext(mapper.readTree(insertJson), "shadow_");
    
    List<MongoDbChangeEventContext> events = java.util.Arrays.asList(event);
    PCollection<MongoDbChangeEventContext> input = pipeline.apply(Create.of(events));
    
    PCollectionTuple udfPreparation = input.apply(
        "Prepare UDF Input",
        ParDo.of(new DataStreamMongoDBToFirestore.ExtractUdfInputFn())
            .withOutputTags(
                DataStreamMongoDBToFirestore.ExtractUdfInputFn.UDF_INPUT_TAG,
                TupleTagList.of(DataStreamMongoDBToFirestore.ExtractUdfInputFn.DELETES_TAG)));
                
    PCollection<FailsafeElement<MongoDbChangeEventContext, String>> udfInput = udfPreparation.get(DataStreamMongoDBToFirestore.ExtractUdfInputFn.UDF_INPUT_TAG);
    udfInput.setCoder(FailsafeElementCoder.of(SerializableCoder.of(MongoDbChangeEventContext.class), StringUtf8Coder.of()));
    
    TupleTag<FailsafeElement<MongoDbChangeEventContext, String>> successTag = new TupleTag<>();
    
    PCollectionTuple udfResult = udfInput.apply(
        "Simulate Failing UDF",
        ParDo.of(new SimulateFailingUdfFn(UDF_EXECUTION_FAILURE_TAG))
            .withOutputTags(successTag, TupleTagList.of(UDF_EXECUTION_FAILURE_TAG)));
            
    udfResult.get(successTag).setCoder(FailsafeElementCoder.of(SerializableCoder.of(MongoDbChangeEventContext.class), StringUtf8Coder.of()));
    udfResult.get(UDF_EXECUTION_FAILURE_TAG).setCoder(FailsafeElementCoder.of(SerializableCoder.of(MongoDbChangeEventContext.class), StringUtf8Coder.of()));
            
    PAssert.that(udfResult.get(UDF_EXECUTION_FAILURE_TAG))
        .satisfies(
            collection -> {
              FailsafeElement<MongoDbChangeEventContext, String> result = collection.iterator().next();
              assertEquals("udf execution failed", result.getPayload());
              assertEquals("udf error", result.getErrorMessage());
              return null;
            });
            
    pipeline.run();
  }

  @Test
  public void testWriteFailedUDFToDlq_usesSevereDlq() {
    DataStreamMongoDBToFirestore.Options options = mock(DataStreamMongoDBToFirestore.Options.class);
    DeadLetterQueueManager dlqManager = mock(DeadLetterQueueManager.class);
    
    when(options.getDeadLetterQueueDirectory()).thenReturn("/tmp/dlq");
    when(dlqManager.getSevereDlqDirectoryWithDateTime()).thenReturn("/tmp/dlq/severe/2026/05/07");
    
    FailsafeElement<MongoDbChangeEventContext, String> failureElement = FailsafeElement.of(null, "invalid json");
    PCollection<FailsafeElement<MongoDbChangeEventContext, String>> failures = pipeline.apply(Create.of(java.util.Arrays.asList(failureElement))
            .withCoder(FailsafeElementCoder.of(SerializableCoder.of(MongoDbChangeEventContext.class), StringUtf8Coder.of())));
            
    DataStreamMongoDBToFirestore.writeFailedUDFToDlq(options, failures, dlqManager.getSevereDlqDirectoryWithDateTime() + "udf_execution_failures/", "tmp_udf_execution_failed");
    
    verify(dlqManager).getSevereDlqDirectoryWithDateTime();
    pipeline.run();
  }

  private static class SimulateModifyingUdfFn extends DoFn<FailsafeElement<MongoDbChangeEventContext, String>, FailsafeElement<MongoDbChangeEventContext, String>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      FailsafeElement<MongoDbChangeEventContext, String> el = c.element();
      String modifiedPayload = "{\"myDate\":{\"$date\":\"2023-05-11T15:00:00Z\"},\"myNaN\":{\"$numberDouble\":\"NaN\"},\"myInf\":{\"$numberDouble\":\"Infinity\"},\"transformed\":true}";
      c.output(FailsafeElement.of(el.getOriginalPayload(), modifiedPayload));
    }
  }

  private static class SimulateInvalidUdfOutputFn extends DoFn<FailsafeElement<MongoDbChangeEventContext, String>, FailsafeElement<MongoDbChangeEventContext, String>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      FailsafeElement<MongoDbChangeEventContext, String> el = c.element();
      c.output(FailsafeElement.of(el.getOriginalPayload(), "invalid json"));
    }
  }

  private static class SimulateFailingUdfFn extends DoFn<FailsafeElement<MongoDbChangeEventContext, String>, FailsafeElement<MongoDbChangeEventContext, String>> {
    private final TupleTag<FailsafeElement<MongoDbChangeEventContext, String>> failureTag;

    public SimulateFailingUdfFn(TupleTag<FailsafeElement<MongoDbChangeEventContext, String>> failureTag) {
      this.failureTag = failureTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c, MultiOutputReceiver out) {
      FailsafeElement<MongoDbChangeEventContext, String> el = c.element();
      out.get(failureTag).output(FailsafeElement.of(el.getOriginalPayload(), "udf execution failed").setErrorMessage("udf error"));
    }
  }



  private static class MergeWithSideInputFn extends DoFn<MongoDbChangeEventContext, MongoDbChangeEventContext> {
    private final org.apache.beam.sdk.values.PCollectionView<FailsafeElement<MongoDbChangeEventContext, String>> udfView;
    private final String shadowCollectionPrefix;

    public MergeWithSideInputFn(org.apache.beam.sdk.values.PCollectionView<FailsafeElement<MongoDbChangeEventContext, String>> udfView, String shadowCollectionPrefix) {
      this.udfView = udfView;
      this.shadowCollectionPrefix = shadowCollectionPrefix;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      MongoDbChangeEventContext ctx = c.element();
      FailsafeElement<MongoDbChangeEventContext, String> element = c.sideInput(udfView);
      try {
        org.bson.Document udfData = org.bson.Document.parse(element.getPayload());
        org.bson.Document fullEvent = org.bson.Document.parse(ctx.getDataAsJsonString());
        fullEvent.put("data", udfData);
        
        MongoDbChangeEventContext newCtx = new MongoDbChangeEventContext(ctx.getChangeEvent(), shadowCollectionPrefix);
        newCtx.setModifiedJsonStringData(fullEvent.toJson(org.bson.json.JsonWriterSettings.builder().outputMode(org.bson.json.JsonMode.EXTENDED).build()));
        
        TEST_OUTPUT.add(newCtx);
        c.output(newCtx);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
