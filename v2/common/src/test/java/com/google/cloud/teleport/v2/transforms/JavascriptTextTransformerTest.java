/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.v2.transforms;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptRuntime;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.TransformTextViaJavascript;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.script.ScriptException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link JavascriptTextTransformer}. */
@RunWith(JUnit4.class)
public class JavascriptTextTransformerTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  // Define the TupleTag's here otherwise the anonymous class will force the test method to
  // be serialized.
  private static final TupleTag<FailsafeElement<PubsubMessage, String>> SUCCESS_TAG =
      new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

  private static final TupleTag<FailsafeElement<PubsubMessage, String>> FAILURE_TAG =
      new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

  private static final String RESOURCES_DIR = "JavascriptTextTransformerTest/";

  private static final String TRANSFORM_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "transform.js").getPath();
  private static final String SCRIPT_PARSE_EXCEPTION_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "scriptParseException.js").getPath();

  /**
   * Test {@link JavascriptRuntime#getInvocable} throws IllegalArgumentException if bad filepath.
   *
   * @throws Exception
   */
  @Test
  public void testInvokeThrowsException() throws Exception {
    JavascriptRuntime javascriptRuntime =
        JavascriptRuntime.newBuilder()
            .setFunctionName("transform")
            .setFileSystemPath("gs://some/bad/random/path")
            .build();

    thrown.expect(anyOf(instanceOf(IOException.class), instanceOf(IllegalArgumentException.class)));
    javascriptRuntime.getInvocable();
  }

  /** Test @{link JavscriptRuntime#getInvocable} throws ScriptException if error in script. */
  @Test
  public void testInvokeScriptException() throws Exception {
    JavascriptRuntime javascriptRuntime =
        JavascriptRuntime.newBuilder()
            .setFileSystemPath(SCRIPT_PARSE_EXCEPTION_FILE_PATH)
            .setFunctionName("transform")
            .build();
    thrown.expect(ScriptException.class);
    String data = javascriptRuntime.invoke("{\"answerToLife\": 42}");
  }

  /**
   * Test {@link JavascriptRuntime#invoke(String)} returns transformed data when a good javascript
   * transform function given.
   */
  @Test
  public void testInvokeGood() throws Exception {
    JavascriptRuntime javascriptRuntime =
        JavascriptRuntime.newBuilder()
            .setFileSystemPath(TRANSFORM_FILE_PATH)
            .setFunctionName("transform")
            .build();
    String data = javascriptRuntime.invoke("{\"answerToLife\": 42}");
    Assert.assertEquals("{\"answerToLife\":42,\"someProp\":\"someValue\"}", data);
  }

  /**
   * Test {@link JavascriptRuntime#invoke(String)} errors when undefined data returned from
   * javascript function.
   */
  @Test
  public void testInvokeUndefined() throws Exception {
    JavascriptRuntime javascriptRuntime =
        JavascriptRuntime.newBuilder()
            .setFileSystemPath(TRANSFORM_FILE_PATH)
            .setFunctionName("transformWithFilter")
            .build();
    String data = javascriptRuntime.invoke("{\"answerToLife\": 43}");
    Assert.assertNull(data);
  }

  /**
   * Test {@link TransformTextViaJavascript} returns transformed data when a good javascript
   * transform given.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testDoFnGood() {
    List<String> inJson = Arrays.asList("{\"answerToLife\": 42}");
    List<String> expectedJson = Arrays.asList("{\"answerToLife\":42,\"someProp\":\"someValue\"}");

    PCollection<String> transformedJson =
        pipeline
            .apply("Create", Create.of(inJson))
            .apply(
                TransformTextViaJavascript.newBuilder()
                    .setFileSystemPath(TRANSFORM_FILE_PATH)
                    .setFunctionName("transform")
                    .build());

    PAssert.that(transformedJson).containsInAnyOrder(expectedJson);

    pipeline.run();
  }

  /** Test {@link TransformTextViaJavascript} passes through data when empty strings as args. */
  @Test
  @Category(NeedsRunner.class)
  public void testDoFnPassthroughEmptyStrings() {
    List<String> inJson = Arrays.asList("{\"answerToLife\":    42}");

    PCollection<String> transformedJson =
        pipeline
            .apply("Create", Create.of(inJson))
            .apply(
                TransformTextViaJavascript.newBuilder()
                    .setFunctionName("")
                    .setFileSystemPath("")
                    .build());

    PAssert.that(transformedJson).containsInAnyOrder(inJson);

    pipeline.run();
  }

  /**
   * Test {@link TransformTextViaJavascript} passes through data when null strings as args. when
   * hasInvocable returns false.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testDoFnPassthroughNullStrings() {
    List<String> inJson = Arrays.asList("{\"answerToLife\":    42}");

    PCollection<String> transformedJson =
        pipeline
            .apply("Create", Create.of(inJson))
            .apply(
                TransformTextViaJavascript.newBuilder()
                    .setFunctionName(null)
                    .setFileSystemPath(null)
                    .build());

    PAssert.that(transformedJson).containsInAnyOrder(inJson);

    pipeline.run();
  }

  /**
   * Test {@link TransformTextViaJavascript} passes through data when null ValueProvider as args.
   * when hasInvocable returns false.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testDoFnPassthroughNullValueProvider() {
    List<String> inJson = Arrays.asList("{\"answerToLife\":    42}");

    PCollection<String> transformedJson =
        pipeline
            .apply("Create", Create.of(inJson))
            .apply(
                TransformTextViaJavascript.newBuilder()
                    .setFunctionName(null)
                    .setFileSystemPath(null)
                    .build());

    PAssert.that(transformedJson).containsInAnyOrder(inJson);

    pipeline.run();
  }

  /**
   * Test {@link TransformTextViaJavascript} allows for script to disregard some data by returning
   * null / undefined.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testDoFnDisregards() {
    List<String> inJson = Arrays.asList("{\"answerToLife\":42}", "{\"answerToLife\":43}");
    List<String> expectedJson = Arrays.asList("{\"answerToLife\":42}");

    PCollection<String> transformedJson =
        pipeline
            .apply("Create", Create.of(inJson))
            .apply(
                TransformTextViaJavascript.newBuilder()
                    .setFileSystemPath(TRANSFORM_FILE_PATH)
                    .setFunctionName("transformWithFilter")
                    .build());

    PAssert.that(transformedJson).containsInAnyOrder(expectedJson);

    pipeline.run();
  }

  /** Tests the {@link FailsafeJavascriptUdf} when the input is valid. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeJavaScriptUdfValidInput() {
    // Test input
    final String fileSystemPath = TRANSFORM_FILE_PATH;
    final String functionName = "transform";

    final String payload = "{\"ticker\": \"GOOGL\", \"price\": 1006.94}";
    final Map<String, String> attributes = ImmutableMap.of("id", "0xDb12", "type", "stock");
    final PubsubMessage message = new PubsubMessage(payload.getBytes(), attributes);

    final FailsafeElement<PubsubMessage, String> input = FailsafeElement.of(message, payload);

    // Register the coder for the pipeline. This prevents having to invoke .setCoder() on
    // many transforms.
    FailsafeElementCoder<PubsubMessage, String> coder =
        FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    // Build the pipeline
    PCollectionTuple output =
        pipeline
            .apply("CreateInput", Create.of(input).withCoder(coder))
            .apply(
                "InvokeUdf",
                FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                    .setFileSystemPath(fileSystemPath)
                    .setFunctionName(functionName)
                    .setSuccessTag(SUCCESS_TAG)
                    .setFailureTag(FAILURE_TAG)
                    .build());

    // Assert
    PAssert.that(output.get(SUCCESS_TAG))
        .satisfies(
            collection -> {
              FailsafeElement<PubsubMessage, String> result = collection.iterator().next();
              PubsubMessage resultMessage = result.getOriginalPayload();
              String expectedPayload =
                  "{\"ticker\":\"GOOGL\",\"price\":1006.94,\"someProp\":\"someValue\"}";

              assertThat(new String(resultMessage.getPayload()), is(equalTo(payload)));
              assertThat(resultMessage.getAttributeMap(), is(equalTo(attributes)));
              assertThat(result.getPayload(), is(equalTo(expectedPayload)));
              assertThat(result.getErrorMessage(), is(nullValue()));
              assertThat(result.getStacktrace(), is(nullValue()));
              return null;
            });

    PAssert.that(output.get(FAILURE_TAG)).empty();

    // Execute the test
    pipeline.run();
  }

  /**
   * Tests the {@link FailsafeJavascriptUdf} when it's passed invalid JSON. In this case the UDF
   * should output the input {@link FailsafeElement} to the dead-letter enriched with error
   * information.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeJavaScriptUdfInvalidInput() {
    // Test input
    final String fileSystemPath = TRANSFORM_FILE_PATH;
    final String functionName = "transform";

    final String payload = "\"ticker\": \"GOOGL\", \"price\": 1006.94";
    final Map<String, String> attributes = ImmutableMap.of("id", "0xDb12", "type", "stock");
    final PubsubMessage message = new PubsubMessage(payload.getBytes(), attributes);

    final FailsafeElement<PubsubMessage, String> input = FailsafeElement.of(message, payload);

    // Register the coder for the pipeline. This prevents having to invoke .setCoder() on
    // many transforms.
    FailsafeElementCoder<PubsubMessage, String> coder =
        FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    // Build the pipeline
    PCollectionTuple output =
        pipeline
            .apply("CreateInput", Create.of(input).withCoder(coder))
            .apply(
                "InvokeUdf",
                FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                    .setFileSystemPath(fileSystemPath)
                    .setFunctionName(functionName)
                    .setSuccessTag(SUCCESS_TAG)
                    .setFailureTag(FAILURE_TAG)
                    .build());

    // Assert
    PAssert.that(output.get(SUCCESS_TAG)).empty();
    PAssert.that(output.get(FAILURE_TAG))
        .satisfies(
            collection -> {
              FailsafeElement<PubsubMessage, String> result = collection.iterator().next();
              PubsubMessage resultMessage = result.getOriginalPayload();

              assertThat(new String(resultMessage.getPayload()), is(equalTo(payload)));
              assertThat(resultMessage.getAttributeMap(), is(equalTo(attributes)));
              assertThat(result.getPayload(), is(equalTo(payload)));
              assertThat(result.getErrorMessage(), is(notNullValue()));
              assertThat(result.getStacktrace(), is(notNullValue()));
              return null;
            });

    // Execute the test
    pipeline.run();
  }
}
