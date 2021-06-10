/*
 * Copyright (C) 2021 Google Inc.
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

package com.google.cloud.teleport.v2.templates;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.templates.PubsubProtoToBigQuery.PubSubProtoToBigQueryOptions;
import com.google.common.io.Resources;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.file.Paths;
import org.apache.beam.sdk.testing.TestPipeline;
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
  private TestPipeline pipeline = TestPipeline.create();

  private static final String GENERATED_PROTO_SCHEMA_PATH =
      Paths.get(
              "..", "common", "target", "generated-test-sources", "protobuf", "schema", "schema.pb")
          .toString();

  @Test
  public void testReadPubsubMessagesWithInvalidSchemaPath() {
    PubSubProtoToBigQueryOptions options = getOptions();
    String path = "/some/invalid.path.pb";
    options.setProtoSchemaPath(path);
    options.setProtoFileName("some_file.proto");
    options.setMessageName("SomeMessage");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> PubsubProtoToBigQuery.readPubsubMessages(options));

    assertThat(exception).hasMessageThat().contains(path);
  }

  @Test
  public void testReadPubsubMessagesWithInvalidProtoSchemaContents() {
    PubSubProtoToBigQueryOptions options = getOptions();
    options.setProtoSchemaPath(Resources.getResource("invalid_proto_schema.pb").toString());
    options.setProtoFileName("some_file.proto");
    options.setMessageName("SomeMessage");

    RuntimeException exception =
        assertThrows(
            RuntimeException.class, () -> PubsubProtoToBigQuery.readPubsubMessages(options));

    // TODO(zhoufek): Stop wrapping the exceptions in RuntimeException
    assertThat(exception).hasCauseThat().isInstanceOf(InvalidProtocolBufferException.class);
  }

  @Test
  public void testReadPubsubMessagesWithInvalidProtoFile() {
    PubSubProtoToBigQueryOptions options = getOptions();
    options.setProtoSchemaPath(GENERATED_PROTO_SCHEMA_PATH);
    String badFile = "some_file.proto";
    options.setProtoFileName(badFile);
    options.setMessageName("SomeMessage");

    RuntimeException exception =
        assertThrows(
            RuntimeException.class, () -> PubsubProtoToBigQuery.readPubsubMessages(options));

    assertThat(exception).hasMessageThat().contains(GENERATED_PROTO_SCHEMA_PATH);
    assertThat(exception).hasMessageThat().contains(badFile);
  }

  @Test
  public void testReadPubsubMessagesWithInvalidMessageName() {
    PubSubProtoToBigQueryOptions options = getOptions();
    options.setProtoSchemaPath(GENERATED_PROTO_SCHEMA_PATH);
    String protoFile = "proto_definition.proto";
    options.setProtoFileName(protoFile);
    String badMessage = "ThisMessageDoesNotExist";
    options.setMessageName(badMessage);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class, () -> PubsubProtoToBigQuery.readPubsubMessages(options));

    assertThat(exception).hasMessageThat().contains(GENERATED_PROTO_SCHEMA_PATH);
    assertThat(exception).hasMessageThat().contains(protoFile);
    assertThat(exception).hasMessageThat().contains(badMessage);
  }

  @Test
  public void testWriteBigQueryWithInvalidJsonSchemaPath() {
    PubSubProtoToBigQueryOptions options = getOptions();
    String path = "/some/invalid/path.json";
    options.setBigQueryTableSchemaPath(path);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> PubsubProtoToBigQuery.writeToBigQuery(options));

    assertThat(exception).hasMessageThat().contains(path);
  }

  /** Returns the pipeline options as {@link PubSubProtoToBigQueryOptions}. */
  private PubSubProtoToBigQueryOptions getOptions() {
    return pipeline.getOptions().as(PubSubProtoToBigQueryOptions.class);
  }
}