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

import com.google.cloud.teleport.avro.AvroPubsubMessageRecord;
import com.google.cloud.teleport.templates.PubsubToGenericAvro.PubsubMessageToArchiveDoFn;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Test class for {@link PubsubToGenericAvro}. */
public class PubsubToGenericAvroTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Test {@link AvroPubsubMessageRecord} correctly maps the message. */
  @Test
  @Category(NeedsRunner.class)
  public void testPubsubMessageToArchive() throws Exception {
    // Create the test input.
    byte[] payload = "Laces out Dan!".getBytes();
    Map<String, String> attributes = ImmutableMap.of("id", "Ace");

    PubsubMessage message = new PubsubMessage(payload, attributes);
    Instant timestamp = Instant.now();

    // Apply the ParDo.
    PCollection<AvroPubsubMessageRecord> results =
        pipeline
            .apply(Create.timestamped(TimestampedValue.of(message, timestamp)))
            .apply(ParDo.of(new PubsubMessageToArchiveDoFn()));

    // Assert on the results.
    PAssert.that(results)
        .containsInAnyOrder(
            new AvroPubsubMessageRecord(payload, attributes, timestamp.getMillis()));

    // Run the pipeline.
    pipeline.run();
  }
}
