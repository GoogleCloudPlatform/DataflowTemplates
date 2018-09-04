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

package com.google.cloud.teleport.templates;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link PubsubToPubsub}. */
@RunWith(JUnit4.class)
public final class PubsubToPubsubTest {
  private static List<PubsubMessage> goodTestMessages;
  private static List<PubsubMessage> badTestMessages;
  private static List<PubsubMessage> allTestMessages;
  private static final String FILTER_KEY = "team";
  private static final String FILTER_VALUE = "falcon";

  @Before
  public void setUp() {
    goodTestMessages =
        ImmutableList.of(
            makePubsubMessage("Lets test!", FILTER_KEY, FILTER_VALUE),
            makePubsubMessage("One more test!", FILTER_KEY, FILTER_VALUE),
            makePubsubMessage("And one more!", FILTER_KEY, FILTER_VALUE));

    badTestMessages =
        ImmutableList.of(
            makePubsubMessage("This one has no attribute", null, null),
            makePubsubMessage("This one too", null, null),
            makePubsubMessage("with unknown attribute", "dummy", "value"));

    allTestMessages =
        ImmutableList.<PubsubMessage>builder()
            .addAll(goodTestMessages)
            .addAll(badTestMessages)
            .build();
  }

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Tests whether all messages flow through when no filter is provided. */
  @Test
  @Category(NeedsRunner.class)
  public void testNoInputFilterProvided() {
    PubsubToPubsub.Options options =
        TestPipeline.testingPipelineOptions().as(PubsubToPubsub.Options.class);
    PCollection<Long> pc =
        pipeline
            .apply(Create.of(allTestMessages))
            .apply(ParDo.of(new PubsubToPubsub.ExtractAndFilterEventsFn()))
            .apply(Count.globally());

    PAssert.thatSingleton(pc).isEqualTo(Long.valueOf(allTestMessages.size()));

    pipeline.run(options);
  }

  /** Tests whether only the valid messages flow through when a filter is provided. */
  @Test
  @Category(NeedsRunner.class)
  public void testInputFilterProvided() {
    PubsubToPubsub.Options options =
        TestPipeline.testingPipelineOptions().as(PubsubToPubsub.Options.class);
    PCollection<Long> pc =
        pipeline
            .apply(Create.of(allTestMessages))
            .apply(ParDo.of(new PubsubToPubsub.ExtractAndFilterEventsFn()))
            .apply(Count.globally());

    PAssert.thatSingleton(pc).isEqualTo(Long.valueOf(goodTestMessages.size()));

    options.setFilterKey(ValueProvider.StaticValueProvider.of(FILTER_KEY));
    options.setFilterValue(ValueProvider.StaticValueProvider.of(FILTER_VALUE));

    pipeline.run(options);
  }

  /**
   * Utility method to create test PubsubMessages.
   *
   * @param payloadString String payload for the test message
   * @param attributeKey Header key for the test message
   * @param attributeValue Header value for the test message
   */
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
}
