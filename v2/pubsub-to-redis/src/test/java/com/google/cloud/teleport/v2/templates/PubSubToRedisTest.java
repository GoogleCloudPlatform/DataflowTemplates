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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/** Test cases for the {@link PubSubToRedis} class. */
public class PubSubToRedisTest {
  private static final String RESOURCES_DIR = "PubSubToRedisTest/";
  private static final String TRANSFORM_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "transform.js").getPath();
  private static final String BAD_TRANSFORM_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "transform.js").getPath();
  private static List<PubsubMessage> goodTestMessages;
  private static List<PubsubMessage> badTestMessages;
  private static List<PubsubMessage> nonFilterableTestMessages;
  private static List<PubsubMessage> allTestMessages;
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static PubsubMessage mockPubSubMessage(
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
        new HashMap<>() {
          {
            put("location", "GJ");
            put("name", "Shubh");
            put("age", "26");
            put("color", "white");
            put("coffee", "filter");
          }
        };
    Map<String, String> testAttributeMap2 =
        new HashMap<>() {
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
            mockPubSubMessage(
                "{\"location\":\"IN\", \"name\":\"Allen\", \"age\":\"28\", \"color\":\"red\", \"coffee\":\"cappuccino\"}",
                null,
                null),
            mockPubSubMessage(
                "{\"location\":\"US\", \"name\":\"Kyle\", \"age\":\"29\", \"color\":\"red\", \"coffee\":\"black\"}",
                null,
                null),
            mockPubSubMessage(
                "{\"location\":\"UK\", \"name\":\"Virag\", \"age\":\"30\", \"color\":\"red\", \"coffee\":\"LATTE\"}",
                null,
                null),
            new PubsubMessage(
                "{\"location\":\"IN\", \"name\":\"John\", \"age\":\"28\", \"color\":\"red\", \"coffee\":\"cappuccino\"}"
                    .getBytes(),
                testAttributeMap2),
            new PubsubMessage(new byte[0], testAttributeMap1));

    badTestMessages =
        ImmutableList.of(
            mockPubSubMessage("This is a bad record", null, null),
            mockPubSubMessage("with unknown attribute", "dummy", "value"));

    allTestMessages =
        ImmutableList.<PubsubMessage>builder()
            .addAll(goodTestMessages)
            .addAll(badTestMessages)
            .build();
  }

  /** Tests the {@link PubSubToRedis} pipeline end-to-end with no UDF supplied. */
  @Test
  public void testPubSubToRedisNoUdfE2E() {

    PubSubToRedis.PubSubToRedisOptions options =
        TestPipeline.testingPipelineOptions().as(PubSubToRedis.PubSubToRedisOptions.class);

    options.setJavascriptTextTransformFunctionName(null);
    options.setJavascriptTextTransformGcsPath(null);

    // Execute pipeline
    pipeline.run(options);
  }

  /**
   * Tests the {@link PubSubToRedis} pipeline end-to-end with an empty message payload but
   * attributes populated.
   */
  @Test
  public void testPubSubToRedisOnlyAttributesE2E() {

    PubSubToRedis.PubSubToRedisOptions options =
        TestPipeline.testingPipelineOptions().as(PubSubToRedis.PubSubToRedisOptions.class);

    // Execute pipeline
    pipeline.run(options);
  }
}
