/*
 * Copyright (C) 2026 Google LLC
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.bson.BsonDocument;
import org.bson.Document;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link MongoDbBulkTransforms}. */
@RunWith(JUnit4.class)
public class MongoDbBulkTransformsTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static MongoClient mockClient;
  private static MongoDatabase mockDatabase;
  private static MongoCollection mockCollection;

  @Before
  public void setUp() {
    mockClient = mock(MongoClient.class);
    mockDatabase = mock(MongoDatabase.class);
    mockCollection = mock(MongoCollection.class);

    when(mockClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockDatabase.getCollection(anyString())).thenReturn(mockCollection);
  }

  private static class MockClientFactory
      implements SerializableFunction<String, MongoClient>, Serializable {
    @Override
    public MongoClient apply(String uri) {
      return mockClient;
    }
  }

  private MongoDbChangeEventContext createEventContext(
      String docId, String changeType, boolean isDelete) throws Exception {
    String payload =
        String.format(
            "{"
                + "\"_metadata_source\": {\"collection\": \"users\"},"
                + "\"_id\": \"\\\"%s\\\"\","
                + "\"_metadata_timestamp_seconds\": 1000,"
                + "\"_metadata_timestamp_nanos\": 0,"
                + "\"_metadata_change_type\": \"%s\","
                + "\"data\": \"{\\\"name\\\": \\\"user_%s\\\"}\""
                + "}",
            docId, changeType, docId);
    return new MongoDbChangeEventContext(OBJECT_MAPPER.readTree(payload), "shadow_");
  }

  @Test
  public void testSuccessfulBulkWrites() throws Exception {
    MongoDbChangeEventContext insertEvent = createEventContext("doc1", "INSERT", false);
    MongoDbChangeEventContext deleteEvent = createEventContext("doc2", "DELETE", true);

    when(mockCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenReturn(mock(BulkWriteResult.class));

    PCollectionTuple result =
        pipeline
            .apply(
                Create.of(insertEvent, deleteEvent).withCoder(MongoDbChangeEventContextCoder.of()))
            .apply(
                MongoDbBulkTransforms.bulkWriteWithDlq()
                    .withConnectionString("mongodb://localhost:27017")
                    .withDatabase("test_db")
                    .withBatchSize(2)
                    .withClientFactory(new MockClientFactory()));

    PAssert.that(result.get(MongoDbBulkTransforms.SUCCESSFUL_WRITE_TAG))
        .containsInAnyOrder(insertEvent, deleteEvent);
    PAssert.that(result.get(MongoDbBulkTransforms.FAILED_WRITE_TAG)).empty();
    PAssert.that(result.get(MongoDbBulkTransforms.SEVERE_FAILED_WRITE_TAG)).empty();

    pipeline.run();
  }

  @Test
  public void testErrorCodeClassification_code2_routesToSevereDlq() throws Exception {
    MongoDbChangeEventContext badDoc = createEventContext("bad_doc", "INSERT", false);

    BulkWriteError error =
        new BulkWriteError(
            MongoDbBulkTransforms.ERR_BAD_VALUE,
            "BadValue: value exceeds limit",
            new BsonDocument(),
            0);
    MongoBulkWriteException exception =
        new MongoBulkWriteException(
            mock(BulkWriteResult.class),
            Collections.singletonList(error),
            null,
            new ServerAddress("localhost", 27017),
            Collections.emptySet());

    doThrow(exception).when(mockCollection).bulkWrite(anyList(), any(BulkWriteOptions.class));

    PCollectionTuple result =
        pipeline
            .apply(Create.of(badDoc).withCoder(MongoDbChangeEventContextCoder.of()))
            .apply(
                MongoDbBulkTransforms.bulkWriteWithDlq()
                    .withConnectionString("mongodb://localhost:27017")
                    .withDatabase("test_db")
                    .withBatchSize(1)
                    .withClientFactory(new MockClientFactory()));

    PAssert.that(result.get(MongoDbBulkTransforms.SUCCESSFUL_WRITE_TAG)).empty();
    PAssert.that(result.get(MongoDbBulkTransforms.FAILED_WRITE_TAG)).empty();

    PCollection<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>> severeOut =
        result.get(MongoDbBulkTransforms.SEVERE_FAILED_WRITE_TAG);
    PAssert.that(severeOut)
        .satisfies(
            elements -> {
              int count = 0;
              for (FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> elem :
                  elements) {
                count++;
                assertNotNull(elem);
                assertTrue(elem.getErrorMessage().contains("Code 2"));
              }
              assertEquals(1, count);
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testErrorCodeClassification_code121_routesToSevereDlq() throws Exception {
    MongoDbChangeEventContext invalidDoc = createEventContext("invalid_doc", "UPDATE", false);

    BulkWriteError error =
        new BulkWriteError(
            MongoDbBulkTransforms.ERR_DOCUMENT_VALIDATION_FAILURE,
            "Document failed validation",
            new BsonDocument(),
            0);
    MongoBulkWriteException exception =
        new MongoBulkWriteException(
            mock(BulkWriteResult.class),
            Collections.singletonList(error),
            null,
            new ServerAddress("localhost", 27017),
            Collections.emptySet());

    doThrow(exception).when(mockCollection).bulkWrite(anyList(), any(BulkWriteOptions.class));

    PCollectionTuple result =
        pipeline
            .apply(Create.of(invalidDoc).withCoder(MongoDbChangeEventContextCoder.of()))
            .apply(
                MongoDbBulkTransforms.bulkWriteWithDlq()
                    .withConnectionString("mongodb://localhost:27017")
                    .withDatabase("test_db")
                    .withBatchSize(1)
                    .withClientFactory(new MockClientFactory()));

    PAssert.that(result.get(MongoDbBulkTransforms.SUCCESSFUL_WRITE_TAG)).empty();
    PAssert.that(result.get(MongoDbBulkTransforms.FAILED_WRITE_TAG)).empty();

    PCollection<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>> severeOut =
        result.get(MongoDbBulkTransforms.SEVERE_FAILED_WRITE_TAG);
    PAssert.that(severeOut)
        .satisfies(
            elements -> {
              int count = 0;
              for (FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> elem :
                  elements) {
                count++;
                assertTrue(elem.getErrorMessage().contains("Code 121"));
              }
              assertEquals(1, count);
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testErrorCodeClassification_transientError_retriesAndRoutesToRetryableDlq()
      throws Exception {
    MongoDbChangeEventContext transientDoc = createEventContext("transient_doc", "UPDATE", false);

    BulkWriteError error =
        new BulkWriteError(
            MongoDbBulkTransforms.ERR_WRITE_CONFLICT,
            "WriteConflict: retryable conflict",
            new BsonDocument(),
            0);
    MongoBulkWriteException exception =
        new MongoBulkWriteException(
            mock(BulkWriteResult.class),
            Collections.singletonList(error),
            null,
            new ServerAddress("localhost", 27017),
            Collections.emptySet());

    doThrow(exception).when(mockCollection).bulkWrite(anyList(), any(BulkWriteOptions.class));

    PCollectionTuple result =
        pipeline
            .apply(Create.of(transientDoc).withCoder(MongoDbChangeEventContextCoder.of()))
            .apply(
                MongoDbBulkTransforms.bulkWriteWithDlq()
                    .withConnectionString("mongodb://localhost:27017")
                    .withDatabase("test_db")
                    .withBatchSize(1)
                    .withMaxWriteRetries(1)
                    .withClientFactory(new MockClientFactory()));

    PAssert.that(result.get(MongoDbBulkTransforms.SUCCESSFUL_WRITE_TAG)).empty();
    PAssert.that(result.get(MongoDbBulkTransforms.SEVERE_FAILED_WRITE_TAG)).empty();

    PCollection<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
        retryableOut = result.get(MongoDbBulkTransforms.FAILED_WRITE_TAG);
    PAssert.that(retryableOut)
        .satisfies(
            elements -> {
              int count = 0;
              for (FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> elem :
                  elements) {
                count++;
                assertTrue(
                    elem.getErrorMessage().contains("Transient write error retries exhausted"));
              }
              assertEquals(1, count);
              return null;
            });

    pipeline.run();
  }

  private MongoDbChangeEventContext createEventContextWithTimestamp(
      String docId, long seconds, int nanos, String changeType) throws Exception {
    String payload =
        String.format(
            "{"
                + "\"_metadata_source\": {\"collection\": \"users\"},"
                + "\"_id\": \"\\\"%s\\\"\","
                + "\"_metadata_timestamp_seconds\": %d,"
                + "\"_metadata_timestamp_nanos\": %d,"
                + "\"_metadata_change_type\": \"%s\","
                + "\"_metadata_read_method\": \"cdc\","
                + "\"data\": \"{\\\"name\\\": \\\"user_%s\\\"}\""
                + "}",
            docId, seconds, nanos, changeType, docId);
    return new MongoDbChangeEventContext(OBJECT_MAPPER.readTree(payload), "shadow_");
  }

  @Test
  public void testCoalescedBatch_successfulWrite_emitsActiveAndSupersededEvents() throws Exception {
    MongoDbChangeEventContext v1 = createEventContextWithTimestamp("doc1", 1000L, 0, "INSERT");
    MongoDbChangeEventContext v2 = createEventContextWithTimestamp("doc1", 1000L, 100, "UPDATE");
    MongoDbChangeEventContext v3 = createEventContextWithTimestamp("doc1", 1001L, 0, "UPDATE");

    when(mockCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenReturn(mock(BulkWriteResult.class));

    PCollectionTuple result =
        pipeline
            .apply(Create.of(v1, v2, v3).withCoder(MongoDbChangeEventContextCoder.of()))
            .apply(
                MongoDbBulkTransforms.bulkWriteWithDlq()
                    .withConnectionString("mongodb://localhost:27017")
                    .withDatabase("test_db")
                    .withBatchSize(10)
                    .withClientFactory(new MockClientFactory()));

    PAssert.that(result.get(MongoDbBulkTransforms.SUCCESSFUL_WRITE_TAG))
        .containsInAnyOrder(v1, v2, v3);
    PAssert.that(result.get(MongoDbBulkTransforms.FAILED_WRITE_TAG)).empty();
    PAssert.that(result.get(MongoDbBulkTransforms.SEVERE_FAILED_WRITE_TAG)).empty();

    pipeline.run();
  }

  @Test
  public void testCoalescedBatch_permanentFailure_doesNotEmitSupersededEvents() throws Exception {
    MongoDbChangeEventContext v1 = createEventContextWithTimestamp("bad_doc", 1000L, 0, "INSERT");
    MongoDbChangeEventContext v2 = createEventContextWithTimestamp("bad_doc", 1000L, 100, "UPDATE");

    when(mockCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenAnswer(
            invocation -> {
              List<WriteModel<Document>> ops = invocation.getArgument(0);
              List<BulkWriteError> errors = new ArrayList<>();
              for (int i = 0; i < ops.size(); i++) {
                errors.add(
                    new BulkWriteError(
                        MongoDbBulkTransforms.ERR_BAD_VALUE,
                        "BadValue: value exceeds limit",
                        new BsonDocument(),
                        i));
              }
              throw new MongoBulkWriteException(
                  mock(BulkWriteResult.class),
                  errors,
                  null,
                  new ServerAddress("localhost", 27017),
                  Collections.emptySet());
            });

    PCollectionTuple result =
        pipeline
            .apply(Create.of(v1, v2).withCoder(MongoDbChangeEventContextCoder.of()))
            .apply(
                MongoDbBulkTransforms.bulkWriteWithDlq()
                    .withConnectionString("mongodb://localhost:27017")
                    .withDatabase("test_db")
                    .withBatchSize(10)
                    .withClientFactory(new MockClientFactory()));

    PAssert.that(result.get(MongoDbBulkTransforms.SUCCESSFUL_WRITE_TAG)).empty();
    PAssert.that(result.get(MongoDbBulkTransforms.FAILED_WRITE_TAG)).empty();

    PCollection<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>> severeOut =
        result.get(MongoDbBulkTransforms.SEVERE_FAILED_WRITE_TAG);
    PAssert.that(severeOut)
        .satisfies(
            elements -> {
              int count = 0;
              for (FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> elem :
                  elements) {
                count++;
                assertNotNull(elem);
                assertTrue(elem.getErrorMessage().contains("Code 2"));
              }
              assertTrue(count >= 1);
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testPartialBatchFailure_emitsSupersededEventsOnlyForSuccessfulDocs()
      throws Exception {
    MongoDbChangeEventContext docAv1 = createEventContextWithTimestamp("docA", 1000L, 0, "INSERT");
    MongoDbChangeEventContext docAv2 =
        createEventContextWithTimestamp("docA", 1000L, 100, "UPDATE");
    MongoDbChangeEventContext docBv1 = createEventContextWithTimestamp("docB", 1000L, 0, "INSERT");
    MongoDbChangeEventContext docBv2 =
        createEventContextWithTimestamp("docB", 1000L, 100, "UPDATE");

    when(mockCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenAnswer(
            invocation -> {
              List<WriteModel<Document>> ops = invocation.getArgument(0);
              List<BulkWriteError> errors = new ArrayList<>();
              for (int i = 0; i < ops.size(); i++) {
                WriteModel<Document> op = ops.get(i);
                if (op.toString().contains("docB")) {
                  errors.add(
                      new BulkWriteError(
                          MongoDbBulkTransforms.ERR_BAD_VALUE,
                          "BadValue for docB",
                          new BsonDocument(),
                          i));
                }
              }
              if (!errors.isEmpty()) {
                throw new MongoBulkWriteException(
                    mock(BulkWriteResult.class),
                    errors,
                    null,
                    new ServerAddress("localhost", 27017),
                    Collections.emptySet());
              }
              return mock(BulkWriteResult.class);
            });

    PCollectionTuple result =
        pipeline
            .apply(
                Create.of(docAv1, docAv2, docBv1, docBv2)
                    .withCoder(MongoDbChangeEventContextCoder.of()))
            .apply(
                MongoDbBulkTransforms.bulkWriteWithDlq()
                    .withConnectionString("mongodb://localhost:27017")
                    .withDatabase("test_db")
                    .withBatchSize(10)
                    .withClientFactory(new MockClientFactory()));

    // docA was successful -> docA events should be emitted as successful
    PAssert.that(result.get(MongoDbBulkTransforms.SUCCESSFUL_WRITE_TAG))
        .satisfies(
            elements -> {
              int count = 0;
              for (MongoDbChangeEventContext elem : elements) {
                count++;
                assertEquals("docA", elem.getDocumentId());
              }
              assertTrue(count >= 1);
              return null;
            });
    PAssert.that(result.get(MongoDbBulkTransforms.FAILED_WRITE_TAG)).empty();

    // docB failed -> only docB events should be emitted to severe DLQ, no docA in severe DLQ
    PCollection<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>> severeOut =
        result.get(MongoDbBulkTransforms.SEVERE_FAILED_WRITE_TAG);
    PAssert.that(severeOut)
        .satisfies(
            elements -> {
              int count = 0;
              for (FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> elem :
                  elements) {
                count++;
                assertNotNull(elem);
                assertEquals("docB", elem.getOriginalPayload().getDocumentId());
                assertTrue(elem.getErrorMessage().contains("Code 2"));
              }
              assertTrue(count >= 1);
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testGeneralException_inCollectionOrSetup_routesToDlqWithoutPipelineCrash()
      throws Exception {
    MongoDbChangeEventContext event = createEventContext("doc1", "INSERT", false);

    when(mockDatabase.getCollection(anyString()))
        .thenThrow(new com.mongodb.MongoException(13, "Unauthorized collection access"));

    PCollectionTuple result =
        pipeline
            .apply(Create.of(event).withCoder(MongoDbChangeEventContextCoder.of()))
            .apply(
                MongoDbBulkTransforms.bulkWriteWithDlq()
                    .withConnectionString("mongodb://localhost:27017")
                    .withDatabase("test_db")
                    .withBatchSize(1)
                    .withClientFactory(new MockClientFactory()));

    PAssert.that(result.get(MongoDbBulkTransforms.SUCCESSFUL_WRITE_TAG)).empty();
    PAssert.that(result.get(MongoDbBulkTransforms.FAILED_WRITE_TAG)).empty();

    PCollection<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>> severeOut =
        result.get(MongoDbBulkTransforms.SEVERE_FAILED_WRITE_TAG);
    PAssert.that(severeOut)
        .satisfies(
            elements -> {
              int count = 0;
              for (FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> elem :
                  elements) {
                count++;
                assertNotNull(elem);
                assertTrue(elem.getErrorMessage().contains("Permanent failure"));
              }
              assertEquals(1, count);
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testPartialBatchFailure_transientErrorRetriedAndSucceeds() throws Exception {
    MongoDbChangeEventContext docA = createEventContextWithTimestamp("docA", 1000L, 0, "INSERT");
    MongoDbChangeEventContext docB = createEventContextWithTimestamp("docB", 1000L, 0, "INSERT");

    // First call: docB fails with transient WriteConflict (Code 112), docA succeeds
    // Second call (retry): docB succeeds
    BulkWriteError transientError =
        new BulkWriteError(
            MongoDbBulkTransforms.ERR_WRITE_CONFLICT,
            "WriteConflict",
            new org.bson.BsonDocument(),
            1);
    MongoBulkWriteException partialException =
        new MongoBulkWriteException(
            mock(BulkWriteResult.class),
            Collections.singletonList(transientError),
            null,
            new com.mongodb.ServerAddress("localhost", 27017),
            Collections.emptySet());

    when(mockCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenThrow(partialException)
        .thenReturn(mock(BulkWriteResult.class));

    PCollectionTuple result =
        pipeline
            .apply(Create.of(docA, docB).withCoder(MongoDbChangeEventContextCoder.of()))
            .apply(
                MongoDbBulkTransforms.bulkWriteWithDlq()
                    .withConnectionString("mongodb://localhost:27017")
                    .withDatabase("test_db")
                    .withBatchSize(2)
                    .withClientFactory(new MockClientFactory()));

    // Both should ultimately succeed without routing to DLQ
    PAssert.that(result.get(MongoDbBulkTransforms.SUCCESSFUL_WRITE_TAG))
        .containsInAnyOrder(docA, docB);
    PAssert.that(result.get(MongoDbBulkTransforms.FAILED_WRITE_TAG)).empty();
    PAssert.that(result.get(MongoDbBulkTransforms.SEVERE_FAILED_WRITE_TAG)).empty();

    pipeline.run();
  }

  @Test
  public void testCoalescedBatch_compositeDocumentIdsWithPermutedKeyOrder_areNotCoalesced()
      throws Exception {
    String payloadOrder1V1 =
        "{"
            + "\"_metadata_source\": {\"collection\": \"users\"},"
            + "\"_id\": \"{\\\"a\\\": 1, \\\"b\\\": 2}\","
            + "\"_metadata_timestamp_seconds\": 1000,"
            + "\"_metadata_timestamp_nanos\": 0,"
            + "\"_metadata_change_type\": \"INSERT\","
            + "\"_metadata_read_method\": \"cdc\","
            + "\"data\": \"{\\\"val\\\": \\\"order1_v1\\\"}\""
            + "}";
    String payloadOrder1V2 =
        "{"
            + "\"_metadata_source\": {\"collection\": \"users\"},"
            + "\"_id\": \"{\\\"a\\\": 1, \\\"b\\\": 2}\","
            + "\"_metadata_timestamp_seconds\": 1001,"
            + "\"_metadata_timestamp_nanos\": 0,"
            + "\"_metadata_change_type\": \"UPDATE\","
            + "\"_metadata_read_method\": \"cdc\","
            + "\"data\": \"{\\\"val\\\": \\\"order1_v2\\\"}\""
            + "}";
    String payloadOrder2 =
        "{"
            + "\"_metadata_source\": {\"collection\": \"users\"},"
            + "\"_id\": \"{\\\"b\\\": 2, \\\"a\\\": 1}\","
            + "\"_metadata_timestamp_seconds\": 1002,"
            + "\"_metadata_timestamp_nanos\": 0,"
            + "\"_metadata_change_type\": \"INSERT\","
            + "\"_metadata_read_method\": \"cdc\","
            + "\"data\": \"{\\\"val\\\": \\\"order2\\\"}\""
            + "}";

    MongoDbChangeEventContext e1 =
        new MongoDbChangeEventContext(OBJECT_MAPPER.readTree(payloadOrder1V1), "shadow_");
    MongoDbChangeEventContext e2 =
        new MongoDbChangeEventContext(OBJECT_MAPPER.readTree(payloadOrder1V2), "shadow_");
    MongoDbChangeEventContext e3 =
        new MongoDbChangeEventContext(OBJECT_MAPPER.readTree(payloadOrder2), "shadow_");

    List<Integer> capturedOpCounts = Collections.synchronizedList(new ArrayList<>());
    when(mockCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenAnswer(
            invocation -> {
              List<WriteModel<Document>> ops = invocation.getArgument(0);
              capturedOpCounts.add(ops.size());
              return mock(BulkWriteResult.class);
            });

    MongoDbBulkTransforms.BulkWriteFn fn =
        MongoDbBulkTransforms.BulkWriteFn.builder()
            .withConnectionString("mongodb://localhost:27017")
            .withDatabase("test_db")
            .withBatchSize(3)
            .withClientFactory(new MockClientFactory())
            .build();

    fn.setup();
    try {
      fn.startBundle();
      for (MongoDbChangeEventContext event : Arrays.asList(e1, e2, e3)) {
        DoFn<MongoDbChangeEventContext, MongoDbChangeEventContext>.ProcessContext processCtx =
            mock(DoFn.ProcessContext.class, RETURNS_DEEP_STUBS);
        when(processCtx.element()).thenReturn(event);
        when(processCtx.timestamp()).thenReturn(Instant.ofEpochMilli(1000L));
        fn.processElement(processCtx);
      }
      DoFn<MongoDbChangeEventContext, MongoDbChangeEventContext>.FinishBundleContext finishCtx =
          mock(DoFn.FinishBundleContext.class);
      fn.finishBundle(finishCtx);
    } finally {
      fn.teardown();
    }

    // e1 and e2 share {"a": 1, "b": 2} and coalesce into 1 op, while e3 has {"b": 2, "a": 1}
    // and must remain a separate op (total = 2 ops in the single batch).
    assertEquals(Collections.singletonList(2), capturedOpCounts);
  }

  @Test
  public void testBulkWriteWithDlq_usesCustomBinaryCoder() throws Exception {
    MongoDbChangeEventContext event = createEventContext("doc1", "INSERT", false);
    when(mockCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenReturn(mock(BulkWriteResult.class));

    PCollectionTuple result =
        pipeline
            .apply(Create.of(event).withCoder(MongoDbChangeEventContextCoder.of()))
            .apply(
                MongoDbBulkTransforms.bulkWriteWithDlq()
                    .withConnectionString("mongodb://localhost:27017")
                    .withDatabase("test_db")
                    .withBatchSize(1)
                    .withClientFactory(new MockClientFactory()));

    assertEquals(
        MongoDbChangeEventContextCoder.of(),
        result.get(MongoDbBulkTransforms.SUCCESSFUL_WRITE_TAG).getCoder());

    pipeline.run();
  }

  @Test
  public void testBulkWriteFn_sharesWorkerRateLimiterAcrossDoFnInstances() {
    MongoDbBulkTransforms.clearCachesForTesting();
    MongoDbBulkTransforms.BulkWriteFn fn1 =
        MongoDbBulkTransforms.BulkWriteFn.builder()
            .withConnectionString("mongodb://localhost:27017")
            .withDatabase("test_db")
            .withInitialWriteRatePerWorker(500)
            .withMaxWriteRatePerWorker(2500)
            .withClientFactory(new MockClientFactory())
            .build();
    MongoDbBulkTransforms.BulkWriteFn fn2 =
        MongoDbBulkTransforms.BulkWriteFn.builder()
            .withConnectionString("mongodb://localhost:27017")
            .withDatabase("test_db")
            .withInitialWriteRatePerWorker(500)
            .withMaxWriteRatePerWorker(2500)
            .withClientFactory(new MockClientFactory())
            .build();

    fn1.setup();
    fn2.setup();
    try {
      assertNotNull(fn1.getWorkerRateLimiterState());
      assertSame(fn1.getWorkerRateLimiterState(), fn2.getWorkerRateLimiterState());
    } finally {
      fn1.teardown();
      fn2.teardown();
    }
  }

  @Test
  public void testCloseIfNotShared_preservesCachedClientAndClosesUncachedClient() {
    MongoDbBulkTransforms.clearCachesForTesting();
    MongoClient cachedClient = mock(MongoClient.class);
    MongoClient uncachedClient = mock(MongoClient.class);

    MongoDbBulkTransforms.registerCachedClientForTesting(
        "mongodb://cached-host:27017", cachedClient);

    MongoDbBulkTransforms.closeIfNotShared(cachedClient);
    verify(cachedClient, never()).close();

    MongoDbBulkTransforms.closeIfNotShared(uncachedClient);
    verify(uncachedClient, times(1)).close();

    MongoDbBulkTransforms.clearCachesForTesting();
  }

  @Test
  public void testBulkWriteFn_preservesIncomingBeamTimestampsAndClampsMaxTimestamp()
      throws Exception {
    MongoDbChangeEventContext event1 = createEventContext("doc1", "INSERT", false);
    MongoDbChangeEventContext event2 = createEventContext("doc2", "INSERT", false);

    when(mockCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenReturn(mock(BulkWriteResult.class));

    Instant t1 = Instant.ofEpochMilli(123456789L);
    Instant t2 = Instant.ofEpochMilli(987654321L);

    assertEquals(
        GlobalWindow.TIMESTAMP_MAX_VALUE.minus(Duration.millis(1)),
        MongoDbBulkTransforms.BulkWriteFn.sanitizeBeamTimestamp(GlobalWindow.TIMESTAMP_MAX_VALUE));

    PCollectionTuple result =
        pipeline
            .apply(
                Create.timestamped(TimestampedValue.of(event1, t1), TimestampedValue.of(event2, t2))
                    .withCoder(MongoDbChangeEventContextCoder.of()))
            .apply(
                MongoDbBulkTransforms.bulkWriteWithDlq()
                    .withConnectionString("mongodb://localhost:27017")
                    .withDatabase("test_db")
                    .withBatchSize(10)
                    .withClientFactory(new MockClientFactory()));

    PCollection<Instant> emittedTimestamps =
        result
            .get(MongoDbBulkTransforms.SUCCESSFUL_WRITE_TAG)
            .apply("ExtractTimestamps", ParDo.of(new ExtractTimestampFn()));

    PAssert.that(emittedTimestamps).containsInAnyOrder(t1, t2);

    pipeline.run();
  }

  private static class ExtractTimestampFn extends DoFn<MongoDbChangeEventContext, Instant> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.timestamp());
    }
  }
}
