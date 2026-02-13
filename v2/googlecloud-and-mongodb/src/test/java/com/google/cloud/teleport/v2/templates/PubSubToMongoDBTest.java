/*
 * Copyright (C) 2019 Google LLC
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/** Test cases for the {@link PubSubToMongoDB} class. */
public class PubSubToMongoDBTest {
  private static final String RESOURCES_DIR = "PubSubToMongoDBTest/";
  private static final String TRANSFORM_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "transform.js").getPath();
  private static final String BAD_TRANSFORM_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "transform.js").getPath();
  private static List<PubsubMessage> goodTestMessages;
  private static List<PubsubMessage> badTestMessages;
  private static List<PubsubMessage> nonFilterableTestMessages;
  private static List<PubsubMessage> allTestMessages;
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static PubsubMessage makePubsubMessage(
      String payloadString, String attributeKey, String attributeValue) {
    Map<String, String> attributeMap;
    if (attributeKey != null) {
      attributeMap = Collections.singletonMap(attributeKey, attributeValue);
    } else {
      attributeMap = Collections.EMPTY_MAP;
    }
    return new PubsubMessage(payloadString.getBytes(), attributeMap);
  }

  @Before
  public void setUp() throws Exception {
    Map<String, String> testAttributeMap1 =
        new HashMap<String, String>() {
          {
            put("location", "GJ");
            put("name", "Shubh");
            put("age", "26");
            put("color", "white");
            put("coffee", "filter");
          }
        };
    Map<String, String> testAttributeMap2 =
        new HashMap<String, String>() {
          {
            put("location", "Durban");
            put("name", "Dan");
            put("age", "22");
            put("color", "red");
            put("coffee", "brown");
          }
        };

    goodTestMessages =
        ImmutableList.of(
            makePubsubMessage(
                "{\"location\":\"IN\", \"name\":\"John\", \"age\":\"28\", \"color\":\"red\", \"coffee\":\"cappuccino\"}",
                null,
                null),
            makePubsubMessage(
                "{\"location\":\"US\", \"name\":\"Jane\", \"age\":\"29\", \"color\":\"red\", \"coffee\":\"black\"}",
                null,
                null),
            makePubsubMessage(
                "{\"location\":\"UK\", \"name\":\"Smith\", \"age\":\"30\", \"color\":\"red\", \"coffee\":\"LATTE\"}",
                null,
                null),
            new PubsubMessage(
                "{\"location\":\"IN\", \"name\":\"John\", \"age\":\"28\", \"color\":\"red\", \"coffee\":\"cappuccino\"}"
                    .getBytes(),
                testAttributeMap2),
            new PubsubMessage(new byte[0], testAttributeMap1));

    badTestMessages =
        ImmutableList.of(
            makePubsubMessage("This is a bad record", null, null),
            makePubsubMessage("with unknown attribute", "dummy", "value"));

    allTestMessages =
        ImmutableList.<PubsubMessage>builder()
            .addAll(goodTestMessages)
            .addAll(badTestMessages)
            .build();
  }

  /** Tests the {@link PubSubToMongoDB} pipeline end-to-end with no UDF supplied. */
  @Test
  public void testPubSubToMongoDBNoUdfE2E() {

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();

    coderRegistry.registerCoderForType(
        PubSubToMongoDB.FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(),
        PubSubToMongoDB.FAILSAFE_ELEMENT_CODER);

    coderRegistry.registerCoderForType(
        PubSubToMongoDB.CODER.getEncodedTypeDescriptor(), PubSubToMongoDB.CODER);

    PubSubToMongoDB.Options options =
        TestPipeline.testingPipelineOptions().as(PubSubToMongoDB.Options.class);

    options.setDeadletterTable("test:dataset.table");
    options.setJavascriptTextTransformFunctionName(null);
    options.setJavascriptTextTransformGcsPath(null);

    PCollectionTuple pc =
        pipeline
            .apply(Create.of(goodTestMessages.get(0)))
            .apply(
                PubSubToMongoDB.PubSubMessageToJsonDocument.newBuilder()
                    .setJavascriptTextTransformFunctionName(
                        options.getJavascriptTextTransformFunctionName())
                    .setJavascriptTextTransformGcsPath(options.getJavascriptTextTransformGcsPath())
                    .build());

    PAssert.that(pc.get(PubSubToMongoDB.TRANSFORM_OUT))
        .satisfies(
            collection -> {
              FailsafeElement<PubsubMessage, String> element = collection.iterator().next();
              assertThat(
                  element.getOriginalPayload().getPayload(),
                  is(equalTo(goodTestMessages.get(0).getPayload())));
              return null;
            });

    // Execute pipeline
    pipeline.run(options);
  }

  /** Tests the {@link PubSubToMongoDB} pipeline end-to-end with a UDF. */
  @Test
  public void testPubSubToMongoDBUdfE2E() {
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        PubSubToMongoDB.FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(),
        PubSubToMongoDB.FAILSAFE_ELEMENT_CODER);
    coderRegistry.registerCoderForType(
        PubSubToMongoDB.CODER.getEncodedTypeDescriptor(), PubSubToMongoDB.CODER);

    PubSubToMongoDB.Options options =
        TestPipeline.testingPipelineOptions().as(PubSubToMongoDB.Options.class);

    options.setDeadletterTable("test:dataset.table");
    options.setJavascriptTextTransformFunctionName("transform");
    options.setJavascriptTextTransformGcsPath(TRANSFORM_FILE_PATH);
    options.setJavascriptTextTransformReloadIntervalMinutes(0);

    PCollectionTuple pc =
        pipeline
            .apply(Create.of(goodTestMessages.get(0)))
            .apply(
                PubSubToMongoDB.PubSubMessageToJsonDocument.newBuilder()
                    .setJavascriptTextTransformFunctionName(
                        options.getJavascriptTextTransformFunctionName())
                    .setJavascriptTextTransformGcsPath(options.getJavascriptTextTransformGcsPath())
                    .setJavascriptTextTransformReloadIntervalMinutes(
                        options.getJavascriptTextTransformReloadIntervalMinutes())
                    .build());

    PAssert.that(pc.get(PubSubToMongoDB.TRANSFORM_OUT))
        .satisfies(
            collection -> {
              FailsafeElement<PubsubMessage, String> element = collection.iterator().next();
              assertThat(
                  element.getOriginalPayload().getPayload(),
                  is(equalTo(goodTestMessages.get(0).getPayload())));
              return null;
            });

    // Execute pipeline
    pipeline.run(options);
  }

  /** Tests the {@link PubSubToMongoDB} pipeline end-to-end with a bad UDF. */
  @Test
  public void testPubSubToMongoDBBadUdfE2E() {

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        PubSubToMongoDB.FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(),
        PubSubToMongoDB.FAILSAFE_ELEMENT_CODER);
    coderRegistry.registerCoderForType(
        PubSubToMongoDB.CODER.getEncodedTypeDescriptor(), PubSubToMongoDB.CODER);

    PubSubToMongoDB.Options options =
        TestPipeline.testingPipelineOptions().as(PubSubToMongoDB.Options.class);

    options.setDeadletterTable("test:dataset.table");
    options.setJavascriptTextTransformFunctionName("transformBad");
    options.setJavascriptTextTransformGcsPath(BAD_TRANSFORM_FILE_PATH);

    PCollectionTuple pc =
        pipeline
            .apply(Create.of(badTestMessages.get(0)))
            .apply(
                PubSubToMongoDB.PubSubMessageToJsonDocument.newBuilder()
                    .setJavascriptTextTransformFunctionName(
                        options.getJavascriptTextTransformFunctionName())
                    .setJavascriptTextTransformGcsPath(options.getJavascriptTextTransformGcsPath())
                    .build());

    PAssert.that(pc.get(PubSubToMongoDB.TRANSFORM_DEADLETTER_OUT))
        .satisfies(
            collection -> {
              FailsafeElement<PubsubMessage, String> element = collection.iterator().next();
              assertThat(
                  element.getOriginalPayload().getPayload(),
                  is(equalTo(badTestMessages.get(0).getPayload())));
              return null;
            });

    PAssert.that(pc.get(PubSubToMongoDB.TRANSFORM_OUT)).empty();

    // Execute pipeline
    pipeline.run(options);
  }

  /**
   * Tests the {@link PubSubToMongoDB} pipeline end-to-end with an empty message payload but
   * attributes populated.
   */
  @Test
  public void testPubSubToMongoDBOnlyAttributesE2E() {

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        PubSubToMongoDB.FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(),
        PubSubToMongoDB.FAILSAFE_ELEMENT_CODER);
    coderRegistry.registerCoderForType(
        PubSubToMongoDB.CODER.getEncodedTypeDescriptor(), PubSubToMongoDB.CODER);

    PubSubToMongoDB.Options options =
        TestPipeline.testingPipelineOptions().as(PubSubToMongoDB.Options.class);

    options.setDeadletterTable("test:dataset.table");

    PCollectionTuple pc =
        pipeline
            .apply(Create.of(goodTestMessages.get(goodTestMessages.size() - 1)))
            .apply(
                PubSubToMongoDB.PubSubMessageToJsonDocument.newBuilder()
                    .setJavascriptTextTransformFunctionName(
                        options.getJavascriptTextTransformFunctionName())
                    .setJavascriptTextTransformGcsPath(options.getJavascriptTextTransformGcsPath())
                    .build());

    PAssert.that(pc.get(PubSubToMongoDB.TRANSFORM_OUT))
        .satisfies(
            collection -> {
              FailsafeElement<PubsubMessage, String> element = collection.iterator().next();
              assertThat(
                  new Gson().fromJson(element.getPayload(), HashMap.class),
                  is(equalTo(element.getOriginalPayload().getAttributeMap())));
              return null;
            });

    // Execute pipeline
    pipeline.run(options);
  }
}
