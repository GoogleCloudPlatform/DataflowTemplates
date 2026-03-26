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

import static org.junit.Assert.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test cases for the {@link MqttToPubsub} class. */
@RunWith(JUnit4.class)
public class MqttToPubsubTest {
  static final byte[][] BYTE_ARRAY = new byte[][] {"hi there".getBytes(StandardCharsets.UTF_8)};
  static final List<byte[]> WORDS = Arrays.asList(BYTE_ARRAY);
  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException exception = ExpectedException.none();
  static final String[] RESULT = new String[] {"hi there"};

  @Test
  public void testPipelineTransform() {
    PCollection<byte[]> input = p.apply(Create.of(WORDS));
    PCollection<String> output =
        input.apply("transform", ParDo.of(new MqttToPubsub.ByteToStringTransform()));
    PAssert.that(output).containsInAnyOrder(RESULT);
    p.run();
  }

  @Test
  public void testValidationFail() {
    MqttToPubsub.MqttToPubsubOptions options =
        PipelineOptionsFactory.create().as(MqttToPubsub.MqttToPubsubOptions.class);
    options.setUsername("test");
    options.setPassword("");
    assertThrows(IllegalArgumentException.class, () -> MqttToPubsub.validate(options));
  }

  @Test
  public void testValidationSuccess() {
    MqttToPubsub.MqttToPubsubOptions options =
        PipelineOptionsFactory.create().as(MqttToPubsub.MqttToPubsubOptions.class);
    options.setUsername("test");
    options.setPassword("test");

    // Expected to not throw an exception:
    MqttToPubsub.validate(options);
  }
}
