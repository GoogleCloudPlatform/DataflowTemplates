/*
 * Copyright (C) 2023 Google LLC
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

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaTimestampType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test cases for the {@link AzureEventhubToPubsub} class. */
@RunWith(JUnit4.class)
public class AzureEventhubToPubsubTest {
  static final String RESULT = "testValue";
  static final KafkaRecord<String, String> INPUT_KAFKA =
      new KafkaRecord<>(
          "test", 1, 0, 100000, KafkaTimestampType.LOG_APPEND_TIME, null, KV.of("testKey", RESULT));
  static final KV<String, String> INPUT = INPUT_KAFKA.getKV();
  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void testPipelineTransform() {
    PCollection<KV<String, String>> input = p.apply(Create.of(INPUT));
    PCollection<String> output =
        input.apply("transform", ParDo.of(new AzureEventhubToPubsub.KafkaRecordToString()));
    PAssert.that(output).containsInAnyOrder(RESULT);
    p.run();
  }
}
