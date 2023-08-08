/*
 * Copyright (C) 2021 Google LLC
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

import static com.google.cloud.teleport.v2.templates.PubsubProtoToBigQuery.runUdf;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.templates.PubsubProtoToBigQuery.PubSubProtoToBigQueryOptions;
import com.google.common.io.Resources;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.file.Paths;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for helper methods for {@link PubsubProtoToBigQuery}.
 *
 * <p>This class is not intended to run the pipeline, just to make sure that expected errors arise
 * during pipeline construction.
 */
@RunWith(JUnit4.class)
public final class PubsubProtoToBigQueryTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final String GENERATED_PROTO_SCHEMA_PATH =
      Paths.get(
              "..", "common", "target", "generated-test-sources", "protobuf", "schema", "schema.pb")
          .toString();

  @Test
  public void testGetDescriptorWithInvalidSchemaPath() {
    PubSubProtoToBigQueryOptions options = getOptions();
    String path = "/some/invalid.path.pb";
    options.setProtoSchemaPath(path);
    options.setFullMessageName("some.message.Name");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> PubsubProtoToBigQuery.getDescriptor(options));

    assertThat(exception).hasMessageThat().contains(path);
  }

  @Test
  public void testGetDescriptorWithInvalidProtoSchemaContents() {
    PubSubProtoToBigQueryOptions options = getOptions();
    options.setProtoSchemaPath(Resources.getResource("invalid_proto_schema.pb").toString());
    options.setFullMessageName("some.message.Name");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> PubsubProtoToBigQuery.getDescriptor(options));

    assertThat(exception).hasCauseThat().isInstanceOf(InvalidProtocolBufferException.class);
  }

  @Test
  public void testGetDescriptorWithInvalidMessageName() {
    PubSubProtoToBigQueryOptions options = getOptions();
    options.setProtoSchemaPath(GENERATED_PROTO_SCHEMA_PATH);
    String badMessageName = "invalid.message.Name";
    options.setFullMessageName(badMessageName);

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> PubsubProtoToBigQuery.getDescriptor(options));

    assertThat(exception).hasMessageThat().contains(GENERATED_PROTO_SCHEMA_PATH);
    assertThat(exception).hasMessageThat().contains(badMessageName);
  }

  @Test
  public void testApplyUdfWithNoUdfPathSet() {
    PubSubProtoToBigQueryOptions options = getOptions();
    ImmutableList<String> inputs = ImmutableList.of("First", "Second", "Third");
    PCollection<String> pInput = pipeline.apply(Create.of(inputs));

    PAssert.that(runUdf(pInput, options)).containsInAnyOrder(inputs);
    pipeline.run();
  }

  @Test
  public void testApplyUdfWithPathButNoFunction() {
    PubSubProtoToBigQueryOptions options = getOptions();
    options.setJavascriptTextTransformGcsPath("/some/path.js");
    options.setJavascriptTextTransformFunctionName("");
    PCollection<String> input = pipeline.apply(Create.of(""));

    assertThrows(IllegalArgumentException.class, () -> runUdf(input, options));
    options.setJavascriptTextTransformFunctionName("");
    assertThrows(IllegalArgumentException.class, () -> runUdf(input, options));

    pipeline.run();
  }

  @Test
  public void testWriteBigQueryWithInvalidJsonSchemaPath() {
    PubSubProtoToBigQueryOptions options = getOptions();
    String path = "/some/invalid/path.json";
    options.setBigQueryTableSchemaPath(path);

    IllegalArgumentException exception =
        assertThrows(
            // Can pass a null descriptor, since it shouldn't be used.
            IllegalArgumentException.class,
            () -> PubsubProtoToBigQuery.writeToBigQuery(options, null));

    assertThat(exception).hasMessageThat().contains(path);
  }

  /** Returns the pipeline options as {@link PubSubProtoToBigQueryOptions}. */
  private PubSubProtoToBigQueryOptions getOptions() {
    return pipeline.getOptions().as(PubSubProtoToBigQueryOptions.class);
  }
}
