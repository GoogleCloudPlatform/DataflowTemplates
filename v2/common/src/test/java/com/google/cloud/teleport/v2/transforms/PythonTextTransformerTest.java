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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.PythonTextTransformer.FailsafePythonUdf;
import com.google.cloud.teleport.v2.transforms.PythonTextTransformer.PythonRuntime;
import com.google.cloud.teleport.v2.transforms.PythonTextTransformer.TransformTextViaPython;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit tests for {@link PythonTextTransformer}. */
@RunWith(JUnit4.class)
public class PythonTextTransformerTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final Logger LOG = LoggerFactory.getLogger(PythonTextTransformer.class);
  // Define the TupleTag's here otherwise the anonymous class will force the test method to
  // be serialized.
  private static final TupleTag<FailsafeElement<PubsubMessage, String>> SUCCESS_TAG =
      new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

  private static final TupleTag<FailsafeElement<PubsubMessage, String>> FAILURE_TAG =
      new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

  private static final String PYTHON_VERSION = "python3";

  private static final String RESOURCES_DIR = "PythonTextTransformerTest/";

  private static final String TRANSFORM_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "transform.py").getPath();


  /**
   * Test {@link PythonRuntime#invoke(String)} returns transformed data when a good python
   * transform function given. Requires installed python3 on local worker.
   */
  @Ignore
  @Test
  public void testInvokeGood() throws Exception {
    PythonRuntime pythonRuntime =
        PythonRuntime.newBuilder()
            .setFileSystemPath(TRANSFORM_FILE_PATH)
            .setFunctionName("transform")
            .setRuntimeVersion(PYTHON_VERSION)
            .build();
    List<String> expectedJson = Arrays.asList("{\"answerToLife\": 42, \"someProp\": \"someValue\"}");
    List<String> data = pythonRuntime.invoke(Arrays.asList("{\"answerToLife\": 42}"), 5);
    Assert.assertEquals(expectedJson, data);
  }

  @Ignore
  @Test
  @Category(NeedsRunner.class)
  public void testDoFnGood() {
    List<String> inJson = Arrays.asList("{\"answerToLife\": 42}");
    List<String> expectedJson = Arrays.asList("{\"answerToLife\": 42, \"someProp\": \"someValue\"}");

    PCollection<String> transformedJson =
        pipeline
            .apply("Create", Create.of(inJson))
            .apply(
                TransformTextViaPython.newBuilder()
                    .setFileSystemPath(TRANSFORM_FILE_PATH)
                    .setFunctionName("transform")
                    .setRuntimeVersion(PYTHON_VERSION)
                    .build());

    PAssert.that(transformedJson).containsInAnyOrder(expectedJson);

    pipeline.run();
  }

  /** Tests the {@link FailsafePythonUdf} when the input is valid. */
  @Ignore
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafePythonValidInput() {
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
                FailsafePythonUdf.<PubsubMessage>newBuilder()
                    .setFileSystemPath(fileSystemPath)
                    .setFunctionName(functionName)
                    .setRuntimeVersion(PYTHON_VERSION)
                    .setSuccessTag(SUCCESS_TAG)
                    .setFailureTag(FAILURE_TAG)
                    .build());
    output.get(SUCCESS_TAG).setCoder(coder);
    output.get(FAILURE_TAG).setCoder(coder);

    // Assert
    PAssert.that(output.get(SUCCESS_TAG))
        .satisfies(
            collection -> {
              FailsafeElement<PubsubMessage, String> result = collection.iterator().next();
              PubsubMessage resultMessage = result.getOriginalPayload();
              String expectedPayload =
                  "{\"ticker\": \"GOOGL\", \"price\": 1006.94, \"someProp\": \"someValue\"}";
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
}
