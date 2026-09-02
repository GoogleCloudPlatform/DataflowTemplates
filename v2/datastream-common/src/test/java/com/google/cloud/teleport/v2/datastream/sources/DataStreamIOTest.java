/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.datastream.sources;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the scalable Datastream FileIO.
 *
 * <p>Tests that require GCS access have been marked to Ignore, but should be run when developing
 * locally.
 */
@RunWith(JUnit4.class)
public class DataStreamIOTest {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamIOTest.class);

  public static final String BUCKET = "gs://ds-fileio-tests/";
  public static final String ROOT_PATH_WITH_DIRECTORIES = "path-with-directories/";
  public static final String ROOT_PATH_WITH_FILES = "path-with-files/";

  @Rule public final transient TestPipeline testPipeline = TestPipeline.create();

  @Ignore
  @Test
  public void testFullContinuous() {
    Pipeline pipeline = Pipeline.create();
    DataStreamIO dsIo = new DataStreamIO(null, BUCKET, "avro", null, null);

    pipeline.apply(dsIo);

    PAssert.that(dsIo.directories)
        .containsInAnyOrder(
            "gs://ds-fileio-tests/path-with-files/HR_JOBS/2020/07/14/11/03/",
            "gs://ds-fileio-tests/path-with-files/HR_JOBS/2020/07/14/12/16/");
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testExtractGcsFile_missingEventType_routesToDlq() {
    PubsubMessage message =
        new PubsubMessage(
            "{}".getBytes(StandardCharsets.UTF_8),
            ImmutableMap.of("bucketId", "test-bucket", "objectId", "test-object.json"),
            "msg-1");

    PCollectionTuple results =
        testPipeline
            .apply(
                "CreateMalformedMessage",
                Create.of(message).withCoder(PubsubMessageWithAttributesAndMessageIdCoder.of()))
            .apply(
                "ExtractGcsFile",
                ParDo.of(new DataStreamIO.ExtractGcsFile(DataStreamIO.GCS_PUBSUB_DLQ_TAG))
                    .withOutputTags(
                        DataStreamIO.GCS_FILE_METADATA_MAIN_TAG,
                        TupleTagList.of(DataStreamIO.GCS_PUBSUB_DLQ_TAG)));

    PAssert.that(results.get(DataStreamIO.GCS_FILE_METADATA_MAIN_TAG)).empty();
    PAssert.that(results.get(DataStreamIO.GCS_PUBSUB_DLQ_TAG))
        .satisfies(
            (SerializableFunction<Iterable<String>, Void>)
                elements -> {
                  List<String> list = Lists.newArrayList(elements);
                  assertEquals(1, list.size());
                  try {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode node = mapper.readTree(list.get(0));
                    assertEquals(
                        "Missing required attribute 'eventType'",
                        node.get("error_message").asText());
                    assertEquals("msg-1", node.get("message_id").asText());
                    assertNotNull(node.get("timestamp"));
                    assertEquals(
                        "test-bucket", node.get("attributes").get("bucketId").asText());
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  return null;
                });

    testPipeline.run();
  }

  @Test
  public void testExtractGcsFile_missingBucketId_routesToDlq() {
    PubsubMessage message =
        new PubsubMessage(
            "{}".getBytes(StandardCharsets.UTF_8),
            ImmutableMap.of("eventType", "OBJECT_FINALIZE", "objectId", "test-object.json"),
            "msg-2");

    PCollectionTuple results =
        testPipeline
            .apply(
                "CreateMissingBucketMessage",
                Create.of(message).withCoder(PubsubMessageWithAttributesAndMessageIdCoder.of()))
            .apply(
                "ExtractGcsFile",
                ParDo.of(new DataStreamIO.ExtractGcsFile(DataStreamIO.GCS_PUBSUB_DLQ_TAG))
                    .withOutputTags(
                        DataStreamIO.GCS_FILE_METADATA_MAIN_TAG,
                        TupleTagList.of(DataStreamIO.GCS_PUBSUB_DLQ_TAG)));

    PAssert.that(results.get(DataStreamIO.GCS_FILE_METADATA_MAIN_TAG)).empty();
    PAssert.that(results.get(DataStreamIO.GCS_PUBSUB_DLQ_TAG))
        .satisfies(
            (SerializableFunction<Iterable<String>, Void>)
                elements -> {
                  List<String> list = Lists.newArrayList(elements);
                  assertEquals(1, list.size());
                  try {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode node = mapper.readTree(list.get(0));
                    assertEquals(
                        "Missing required attribute 'bucketId' for OBJECT_FINALIZE",
                        node.get("error_message").asText());
                    assertEquals("msg-2", node.get("message_id").asText());
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  return null;
                });

    testPipeline.run();
  }

  @Test
  public void testExtractGcsFile_missingObjectId_routesToDlq() {
    PubsubMessage message =
        new PubsubMessage(
            "{}".getBytes(StandardCharsets.UTF_8),
            ImmutableMap.of("eventType", "OBJECT_FINALIZE", "bucketId", "test-bucket"),
            "msg-3");

    PCollectionTuple results =
        testPipeline
            .apply(
                "CreateMissingObjectMessage",
                Create.of(message).withCoder(PubsubMessageWithAttributesAndMessageIdCoder.of()))
            .apply(
                "ExtractGcsFile",
                ParDo.of(new DataStreamIO.ExtractGcsFile(DataStreamIO.GCS_PUBSUB_DLQ_TAG))
                    .withOutputTags(
                        DataStreamIO.GCS_FILE_METADATA_MAIN_TAG,
                        TupleTagList.of(DataStreamIO.GCS_PUBSUB_DLQ_TAG)));

    PAssert.that(results.get(DataStreamIO.GCS_FILE_METADATA_MAIN_TAG)).empty();
    PAssert.that(results.get(DataStreamIO.GCS_PUBSUB_DLQ_TAG))
        .satisfies(
            (SerializableFunction<Iterable<String>, Void>)
                elements -> {
                  List<String> list = Lists.newArrayList(elements);
                  assertEquals(1, list.size());
                  try {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode node = mapper.readTree(list.get(0));
                    assertEquals(
                        "Missing required attribute 'objectId' for OBJECT_FINALIZE",
                        node.get("error_message").asText());
                    assertEquals("msg-3", node.get("message_id").asText());
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  return null;
                });

    testPipeline.run();
  }

  @Test
  public void testExtractGcsFile_unrecognizedEventType_routesToDlq() {
    PubsubMessage message =
        new PubsubMessage(
            "{}".getBytes(StandardCharsets.UTF_8),
            ImmutableMap.of(
                "eventType",
                "CUSTOM_NOTIFICATION",
                "bucketId",
                "test-bucket",
                "objectId",
                "test-object.json"));

    PCollectionTuple results =
        testPipeline
            .apply(
                "CreateUnrecognizedMessage",
                Create.of(message).withCoder(PubsubMessageWithAttributesCoder.of()))
            .apply(
                "ExtractGcsFile",
                ParDo.of(new DataStreamIO.ExtractGcsFile(DataStreamIO.GCS_PUBSUB_DLQ_TAG))
                    .withOutputTags(
                        DataStreamIO.GCS_FILE_METADATA_MAIN_TAG,
                        TupleTagList.of(DataStreamIO.GCS_PUBSUB_DLQ_TAG)));

    PAssert.that(results.get(DataStreamIO.GCS_FILE_METADATA_MAIN_TAG)).empty();
    PAssert.that(results.get(DataStreamIO.GCS_PUBSUB_DLQ_TAG))
        .satisfies(
            (SerializableFunction<Iterable<String>, Void>)
                elements -> {
                  List<String> list = Lists.newArrayList(elements);
                  assertEquals(1, list.size());
                  try {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode node = mapper.readTree(list.get(0));
                    assertEquals(
                        "Unrecognized GCS eventType: CUSTOM_NOTIFICATION",
                        node.get("error_message").asText());
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  return null;
                });

    testPipeline.run();
  }

  @Test
  public void testExtractGcsFile_nonFinalizeGcsEvents_ignoredCleanly() {
    PubsubMessage deleteMsg =
        new PubsubMessage(
            new byte[0],
            ImmutableMap.of(
                "eventType",
                "OBJECT_DELETE",
                "bucketId",
                "test-bucket",
                "objectId",
                "test-object.json"));
    PubsubMessage metadataMsg =
        new PubsubMessage(
            new byte[0],
            ImmutableMap.of(
                "eventType",
                "OBJECT_METADATA_UPDATE",
                "bucketId",
                "test-bucket",
                "objectId",
                "test-object.json"));
    PubsubMessage archiveMsg =
        new PubsubMessage(
            new byte[0],
            ImmutableMap.of(
                "eventType",
                "OBJECT_ARCHIVE",
                "bucketId",
                "test-bucket",
                "objectId",
                "test-object.json"));

    PCollectionTuple results =
        testPipeline
            .apply(
                "CreateNonFinalizeMessages",
                Create.of(deleteMsg, metadataMsg, archiveMsg)
                    .withCoder(PubsubMessageWithAttributesCoder.of()))
            .apply(
                "ExtractGcsFile",
                ParDo.of(new DataStreamIO.ExtractGcsFile(DataStreamIO.GCS_PUBSUB_DLQ_TAG))
                    .withOutputTags(
                        DataStreamIO.GCS_FILE_METADATA_MAIN_TAG,
                        TupleTagList.of(DataStreamIO.GCS_PUBSUB_DLQ_TAG)));

    PAssert.that(results.get(DataStreamIO.GCS_FILE_METADATA_MAIN_TAG)).empty();
    PAssert.that(results.get(DataStreamIO.GCS_PUBSUB_DLQ_TAG)).empty();

    testPipeline.run();
  }

  @Test
  public void testExtractGcsFile_directoryNotification_ignoredCleanly() {
    PubsubMessage dirMsg =
        new PubsubMessage(
            new byte[0],
            ImmutableMap.of(
                "eventType",
                "OBJECT_FINALIZE",
                "bucketId",
                "test-bucket",
                "objectId",
                "test-dir/"));

    PCollectionTuple results =
        testPipeline
            .apply(
                "CreateDirMessage",
                Create.of(dirMsg).withCoder(PubsubMessageWithAttributesCoder.of()))
            .apply(
                "ExtractGcsFile",
                ParDo.of(new DataStreamIO.ExtractGcsFile(DataStreamIO.GCS_PUBSUB_DLQ_TAG))
                    .withOutputTags(
                        DataStreamIO.GCS_FILE_METADATA_MAIN_TAG,
                        TupleTagList.of(DataStreamIO.GCS_PUBSUB_DLQ_TAG)));

    PAssert.that(results.get(DataStreamIO.GCS_FILE_METADATA_MAIN_TAG)).empty();
    PAssert.that(results.get(DataStreamIO.GCS_PUBSUB_DLQ_TAG)).empty();

    testPipeline.run();
  }

  @Test
  public void testExtractGcsFile_withoutDlqTag_doesNotThrow() {
    PubsubMessage malformedMsg =
        new PubsubMessage(
            "{}".getBytes(StandardCharsets.UTF_8),
            Collections.emptyMap());

    PCollection<?> output =
        testPipeline
            .apply(
                "CreateMalformedWithoutDlq",
                Create.of(malformedMsg).withCoder(PubsubMessageWithAttributesCoder.of()))
            .apply(
                "ExtractGcsFileWithoutDlq",
                ParDo.of(new DataStreamIO.ExtractGcsFile()));

    PAssert.that(output).empty();
    testPipeline.run();
  }

  @Test
  public void testDataStreamIO_dlqBuilderMethods() {
    DataStreamIO io =
        new DataStreamIO(null, BUCKET, "avro", null, null)
            .withDlqDirectory("gs://test-bucket/dlq")
            .withDatastreamSourceType("oracle");

    assertEquals("gs://test-bucket/dlq", io.getDlqDirectory());
  }
}
