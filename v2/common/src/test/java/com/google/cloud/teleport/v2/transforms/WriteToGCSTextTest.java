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
package com.google.cloud.teleport.v2.transforms;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test cases for the {@link WriteToGCSText} class. */
@RunWith(JUnit4.class)
public class WriteToGCSTextTest {

  /** Rule for pipeline testing. */
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Rule for exception testing. */
  @Rule public ExpectedException expectedException = ExpectedException.none();

  private static final String FAKE_DIR = "FakeDirectory/";

  private static final String TEXT_FILENAME_PREFIX = "text-output-";

  private static final Integer NUM_SHARDS = 1;

  private static final String FAKE_TEMP_LOCATION = "FakeTempLocation/";

  // Create the test input.
  private static final String key = "GenericRecord";
  private static final String value = "Hey Bob";
  private static final KV<String, String> message = KV.of(key, value);

  /** Test whether {@link WriteToGCSText} throws an exception if no output directory is provided. */
  @Test
  public void testWriteWithoutOutputDirectory() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("withOutputDirectory(outputDirectory) called with null input.");

    pipeline
        .apply(
            "CreateInput",
            Create.of(message).withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
        .apply(
            "WriteTextFile(s)",
            WriteToGCSText.newBuilder()
                .withOutputDirectory(null)
                .withOutputFilenamePrefix(TEXT_FILENAME_PREFIX)
                .setNumShards(NUM_SHARDS)
                .withTempLocation(FAKE_TEMP_LOCATION)
                .build());
    pipeline.run();
  }

  /**
   * Test whether {@link WriteToGCSText} throws an exception if temporary directory is not provided.
   */
  @Test
  public void testWriteWithoutTempLocation() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("withTempLocation(tempLocation) called with null input. ");

    pipeline
        .apply(
            "CreateInput",
            Create.of(message).withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
        .apply(
            "WriteTextFile(s)",
            WriteToGCSText.newBuilder()
                .withOutputDirectory(FAKE_DIR)
                .withOutputFilenamePrefix(TEXT_FILENAME_PREFIX)
                .setNumShards(NUM_SHARDS)
                .withTempLocation(null)
                .build());
    pipeline.run();
  }
}
