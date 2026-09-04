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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MongoDbTransformsTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final transient TemporaryFolder tempFolder = new TemporaryFolder();

  // Static fields to avoid serialization issues in Beam tests
  private static MongoClient staticClient;
  private static MongoDatabase staticDatabase;
  private static MongoCollection<Document> staticCollection;
  private static final TupleTag<DocumentWithMetadata> MAIN_TAG =
      new TupleTag<DocumentWithMetadata>() {};
  private static final TupleTag<DocumentWithMetadata> FAILURE_TAG =
      new TupleTag<DocumentWithMetadata>() {};

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    staticClient = mock(MongoClient.class);
    staticDatabase = mock(MongoDatabase.class);
    staticCollection = mock(MongoCollection.class);

    when(staticClient.getDatabase(anyString())).thenReturn(staticDatabase);
    when(staticDatabase.getCollection(anyString())).thenReturn(staticCollection);
  }

  @Test
  public void writeWithDlq_transientError_retries() {
    AtomicInteger callCount = new AtomicInteger(0);
    when(staticCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenAnswer(
            invocation -> {
              if (callCount.getAndIncrement() == 0) {
                throw new MongoBulkWriteException(
                    mock(BulkWriteResult.class),
                    Arrays.asList(new BulkWriteError(11600, "Interrupted", new BsonDocument(), 0)),
                    null,
                    new ServerAddress(),
                    Collections.emptySet());
              }
              return mock(BulkWriteResult.class);
            });
    PCollection<DocumentWithMetadata> input =
        pipeline.apply(Create.of(DocumentWithMetadata.of(new Document("_id", 1), "test", "test")));

    input.apply(
        "Write_Transient",
        MongoDbTransforms.writeWithDlq()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withMaxWriteRetries(3)
            .withBatchSize(1)
            .withClientFactory(new MockClientFactory()));
    PipelineResult result = pipeline.run();

    assertEquals(2, callCount.get());
    assertSuccessCount(result, 1L);
    assertCounter(result, "inMemoryRetries", 1L);
    assertCounter(result, "inMemoryRetries_MongoBulkWriteException_11600", 1L);
  }

  @Test
  public void writeWithDlq_permanentError_noRetry() {
    AtomicInteger callCount = new AtomicInteger(0);
    when(staticCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenAnswer(
            invocation -> {
              callCount.getAndIncrement();
              throw new MongoBulkWriteException(
                  mock(BulkWriteResult.class),
                  Arrays.asList(new BulkWriteError(11000, "Duplicate Key", new BsonDocument(), 0)),
                  null,
                  new ServerAddress(),
                  Collections.emptySet());
            });

    PCollection<DocumentWithMetadata> input =
        pipeline.apply(Create.of(DocumentWithMetadata.of(new Document("_id", 1), "test", "test")));

    input.apply(
        "Write_Permanent",
        MongoDbTransforms.writeWithDlq()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withMaxWriteRetries(3)
            .withBatchSize(1)
            .withClientFactory(new MockClientFactory()));
    PipelineResult result = pipeline.run();

    assertEquals(1, callCount.get());
    assertSuccessCount(result, 0L);
    assertCounter(result, "severeFailedWrites", 1L);
    assertCounter(result, "severeFailedWrites_MongoBulkWriteException_11000", 1L);
    assertCounter(result, "permanentFailures", 1L);
  }

  @Test
  public void writeWithDlq_unordered_partialSuccess() {
    AtomicInteger callCount = new AtomicInteger(0);
    when(staticCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenAnswer(
            invocation -> {
              if (callCount.getAndIncrement() == 0) {
                throw new MongoBulkWriteException(
                    mock(BulkWriteResult.class),
                    Arrays.asList(
                        new BulkWriteError(11000, "Duplicate Key", new BsonDocument(), 0)),
                    null,
                    new ServerAddress(),
                    Collections.emptySet());
              }
              return mock(BulkWriteResult.class);
            });

    PCollection<DocumentWithMetadata> input =
        pipeline.apply(
            Create.of(
                DocumentWithMetadata.of(new Document("_id", 1), "test", "test"),
                DocumentWithMetadata.of(new Document("_id", 2), "test", "test")));

    input.apply(
        "Write_Partial",
        MongoDbTransforms.writeWithDlq()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withMaxWriteRetries(3)
            .withBatchSize(2)
            .withClientFactory(new MockClientFactory()));
    PipelineResult result = pipeline.run();

    assertEquals(2, callCount.get());
    assertSuccessCount(result, 1L);
    assertCounter(result, "severeFailedWrites", 1L);
    assertCounter(result, "severeFailedWrites_MongoBulkWriteException_11000", 1L);
    assertCounter(result, "permanentFailures", 1L);
  }

  @Test
  public void writeWithDlq_hundredDocuments_success() {
    when(staticCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenReturn(mock(BulkWriteResult.class));

    DocumentWithMetadata[] docs = new DocumentWithMetadata[100];
    for (int i = 0; i < 100; i++) {
      docs[i] = DocumentWithMetadata.of(new Document("_id", i), "test", "test");
    }
    PCollection<DocumentWithMetadata> input = pipeline.apply(Create.of(Arrays.asList(docs)));

    input.apply(
        "Write_100",
        MongoDbTransforms.writeWithDlq()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withBatchSize(100)
            .withClientFactory(new MockClientFactory()));
    PipelineResult result = pipeline.run();

    assertSuccessCount(result, 100L);
  }

  @Test
  public void writeWithDlq_zeroDocuments_successCountZero() {
    PCollection<DocumentWithMetadata> input =
        pipeline.apply(Create.empty(TypeDescriptor.of(DocumentWithMetadata.class)));

    input.apply(
        "Write_0",
        MongoDbTransforms.writeWithDlq()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withClientFactory(new MockClientFactory()));
    PipelineResult result = pipeline.run();

    assertSuccessCount(result, 0L);
  }

  @Test
  public void writeWithDlq_mixedUpsertAndDelete_success() {
    List<WriteModel<Document>> capturedModels = Collections.synchronizedList(new ArrayList<>());
    when(staticCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenAnswer(
            invocation -> {
              List<WriteModel<Document>> models = invocation.getArgument(0);
              capturedModels.addAll(models);
              return mock(BulkWriteResult.class);
            });

    DocumentWithMetadata upsertDoc =
        DocumentWithMetadata.cdcEvent(
            new Document("_id", "u1").append("name", "Alice"),
            "{\"_id\": \"u1\", \"name\": \"Alice\"}",
            "test",
            "test",
            DocumentWithMetadata.OperationType.INSERT,
            TimestampSortKey.cdc(1000L, 1L),
            "{\"_id\": \"u1\"}");

    DocumentWithMetadata deleteDoc =
        DocumentWithMetadata.cdcEvent(
            null,
            null,
            "test",
            "test",
            DocumentWithMetadata.OperationType.DELETE,
            TimestampSortKey.cdc(1000L, 2L),
            "{\"_id\": \"d1\"}");

    PCollection<DocumentWithMetadata> input = pipeline.apply(Create.of(upsertDoc, deleteDoc));

    input.apply(
        "Write_Mixed",
        MongoDbTransforms.writeWithDlq()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withBatchSize(10)
            .withClientFactory(new MockClientFactory()));
    PipelineResult result = pipeline.run();

    assertSuccessCount(result, 2L);
    assertEquals(2, capturedModels.size());
    assertTrue(capturedModels.stream().anyMatch(m -> m instanceof ReplaceOneModel));
    assertTrue(capturedModels.stream().anyMatch(m -> m instanceof DeleteOneModel));
  }

  @Test
  public void writeWithDlq_deletePermanentFailure_sentToDlqWithMetadata() {
    when(staticCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenThrow(
            new MongoBulkWriteException(
                mock(BulkWriteResult.class),
                Arrays.asList(new BulkWriteError(11000, "Duplicate Key", new BsonDocument(), 0)),
                null,
                new ServerAddress(),
                Collections.emptySet()));

    DocumentWithMetadata deleteDoc =
        DocumentWithMetadata.cdcEvent(
            null,
            null,
            "test",
            "test",
            DocumentWithMetadata.OperationType.DELETE,
            TimestampSortKey.cdc(1000L, 5L),
            "{\"_id\": \"d99\"}");

    PCollection<DocumentWithMetadata> input = pipeline.apply(Create.of(deleteDoc));

    PCollectionTuple tuple =
        input.apply(
            "Write_Delete_Failure",
            ParDo.of(
                    MongoDbTransforms.WriteFn.builder()
                        .withUri("mongodb://localhost:27017")
                        .withDatabase("test")
                        .withBatchSize(1)
                        .withMaxWriteRetries(1)
                        .withDlqMaxRetries(3)
                        .withClientFactory(new MockClientFactory())
                        .withFailureTag(FAILURE_TAG)
                        .build())
                .withOutputTags(MAIN_TAG, TupleTagList.of(FAILURE_TAG)));

    PAssert.that(tuple.get(FAILURE_TAG))
        .satisfies(
            failures -> {
              List<DocumentWithMetadata> list = new ArrayList<>();
              failures.forEach(list::add);
              assertEquals(1, list.size());
              DocumentWithMetadata failedItem = list.get(0);
              assertEquals(DocumentWithMetadata.OperationType.DELETE, failedItem.getOperationType());
              assertEquals("{\"_id\": \"d99\"}", failedItem.getDocumentKey());
              assertEquals(TimestampSortKey.cdc(1000L, 5L), failedItem.getTimestampSortKey());
              assertEquals(DocumentWithMetadata.ErrorType.PERMANENT, failedItem.getErrorType());
              return null;
            });

    pipeline.run();
  }

  @Test
  public void writeWithDlq_dropEvent_skippedWithoutError() {
    DocumentWithMetadata dropDoc =
        DocumentWithMetadata.cdcEvent(
            null,
            null,
            "test",
            "test",
            DocumentWithMetadata.OperationType.DROP,
            TimestampSortKey.cdc(1000L, 10L),
            null);

    PCollection<DocumentWithMetadata> input = pipeline.apply(Create.of(dropDoc));

    input.apply(
        "Write_Drop",
        MongoDbTransforms.writeWithDlq()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withBatchSize(10)
            .withClientFactory(new MockClientFactory()));
    PipelineResult result = pipeline.run();

    assertSuccessCount(result, 0L);
  }

  private long getCounterValue(PipelineResult result, String counterName) {
    for (MetricResult<Long> c :
        result.metrics().queryMetrics(MetricsFilter.builder().build()).getCounters()) {
      if (c.getName().getName().equals(counterName)) {
        return c.getCommitted();
      }
    }
    return 0L;
  }

  private void assertCounter(PipelineResult result, String counterName, long expectedCount) {
    assertEquals(expectedCount, getCounterValue(result, counterName));
  }

  private void assertSuccessCount(PipelineResult result, long expectedCount) {
    assertCounter(result, "successfulWrites", expectedCount);
  }

  @Test
  public void writeWithDlq_documentLevelRetry_partialSuccess() {
    AtomicInteger callCount = new AtomicInteger(0);
    final boolean[] doc2Retried = new boolean[] {false};
    when(staticCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenAnswer(
            invocation -> {
              callCount.getAndIncrement();
              List<WriteModel<Document>> updates = invocation.getArgument(0);
              List<BulkWriteError> errors = new ArrayList<>();
              for (int i = 0; i < updates.size(); i++) {
                Document doc = (Document) ((ReplaceOneModel) updates.get(i)).getReplacement();
                int id = doc.getInteger("_id");
                if (id == 1) {
                  errors.add(new BulkWriteError(11000, "Duplicate Key", new BsonDocument(), i));
                } else if (id == 2) {
                  if (!doc2Retried[0]) {
                    doc2Retried[0] = true;
                    errors.add(new BulkWriteError(11600, "Interrupted", new BsonDocument(), i));
                  }
                }
              }
              if (!errors.isEmpty()) {
                throw new MongoBulkWriteException(
                    mock(BulkWriteResult.class),
                    errors,
                    null,
                    new ServerAddress(),
                    Collections.emptySet());
              }
              return mock(BulkWriteResult.class);
            });

    DocumentWithMetadata doc0 = DocumentWithMetadata.of(new Document("_id", 0), "test", "test");
    DocumentWithMetadata doc1 = DocumentWithMetadata.of(new Document("_id", 1), "test", "test");
    DocumentWithMetadata doc2 = DocumentWithMetadata.of(new Document("_id", 2), "test", "test");

    PCollection<DocumentWithMetadata> input = pipeline.apply(Create.of(doc0, doc1, doc2));

    input.apply(
        "Write_DocLevelRetry",
        ParDo.of(
                MongoDbTransforms.WriteFn.builder()
                    .withUri("mongodb://localhost:27017")
                    .withDatabase("test")
                    .withBatchSize(3)
                    .withMaxWriteRetries(3)
                    .withMaxConcurrentAsyncWrites(1)
                    .withClientFactory(new MockClientFactory())
                    .withFailureTag(FAILURE_TAG)
                    .build())
            .withOutputTags(MAIN_TAG, TupleTagList.of(FAILURE_TAG)));

    PipelineResult result = pipeline.run();

    assertTrue(callCount.get() >= 2);
    assertSuccessCount(result, 2L);
  }

  private static class MockClientFactory implements SerializableFunction<String, MongoClient> {
    @Override
    public MongoClient apply(String input) {
      return staticClient;
    }
  }

  @Test
  public void applyUdfFn_transformsDocument() throws Exception {
    File udfFile = tempFolder.newFile("transform.js");
    try (FileWriter writer = new FileWriter(udfFile)) {
      writer.write(
          "function transform(inJson) {\n"
              + "  var obj = JSON.parse(inJson);\n"
              + "  obj.udf_applied = true;\n"
              + "  return JSON.stringify(obj);\n"
              + "}");
    }

    Document doc = new Document("_id", 1).append("name", "test");
    DocumentWithMetadata input = DocumentWithMetadata.of(doc);

    PCollection<DocumentWithMetadata> inputCollection = pipeline.apply(Create.of(input));

    PCollectionTuple output =
        inputCollection.apply(
            "ApplyUDF",
            ParDo.of(
                    new MongoDbTransforms.ApplyUdfFn(
                        udfFile.getAbsolutePath(), "transform", 0, FAILURE_TAG))
                .withOutputTags(MAIN_TAG, TupleTagList.of(FAILURE_TAG)));

    PAssert.that(output.get(MAIN_TAG))
        .satisfies(
            collection -> {
              DocumentWithMetadata result = collection.iterator().next();
              assertEquals(true, result.getDocument().get("udf_applied"));
              assertEquals("test", result.getDocument().get("name"));
              assertEquals(input.getOriginalDocument(), result.getOriginalDocument());
              return null;
            });

    PAssert.that(output.get(FAILURE_TAG)).empty();

    pipeline.run();
  }

  @Test
  public void applyUdfFn_cdcFullDoc_preservesOriginalDocument() throws Exception {
    File udfFile = tempFolder.newFile("cdc_transform.js");
    try (FileWriter writer = new FileWriter(udfFile)) {
      writer.write(
          "function transform(inJson) {\n"
              + "  var obj = JSON.parse(inJson);\n"
              + "  obj.enriched = 'yes';\n"
              + "  return JSON.stringify(obj);\n"
              + "}");
    }

    Document fullDoc = new Document("_id", 42).append("status", "ACTIVE").append("tier", "GOLD");
    TimestampSortKey sortKey = TimestampSortKey.cdc(1700000000L, 1L);
    DocumentWithMetadata cdcEvent =
        DocumentWithMetadata.cdcEvent(
            fullDoc,
            fullDoc.toJson(),
            "users",
            "users_target",
            DocumentWithMetadata.OperationType.UPDATE,
            sortKey,
            new Document("_id", 42).toJson());

    PCollection<DocumentWithMetadata> inputCollection = pipeline.apply(Create.of(cdcEvent));

    PCollectionTuple output =
        inputCollection.apply(
            "ApplyUDF_CDC",
            ParDo.of(
                    new MongoDbTransforms.ApplyUdfFn(
                        udfFile.getAbsolutePath(), "transform", 0, FAILURE_TAG))
                .withOutputTags(MAIN_TAG, TupleTagList.of(FAILURE_TAG)));

    PAssert.that(output.get(MAIN_TAG))
        .satisfies(
            collection -> {
              DocumentWithMetadata result = collection.iterator().next();
              assertEquals("yes", result.getDocument().get("enriched"));
              assertEquals("ACTIVE", result.getDocument().get("status"));
              assertEquals("GOLD", result.getDocument().get("tier"));
              // Verify that originalDocument retains the untransformed original fullDoc
              assertEquals(fullDoc.toJson(), result.getOriginalDocument());
              // Verify that CDC metadata is preserved
              assertEquals(DocumentWithMetadata.OperationType.UPDATE, result.getOperationType());
              assertEquals(sortKey, result.getTimestampSortKey());
              assertEquals("users", result.getSourceCollection());
              assertEquals("users_target", result.getTargetCollection());
              return null;
            });

    PAssert.that(output.get(FAILURE_TAG)).empty();

    pipeline.run();
  }

  @Test
  public void applyUdfFn_failure_routesToDlq() throws Exception {
    File udfFile = tempFolder.newFile("transform_fail.js");
    try (FileWriter writer = new FileWriter(udfFile)) {
      writer.write(
          "function transform(inJson) {\n" + "  throw 'UDF failed intentionally';\n" + "}");
    }

    Document doc = new Document("_id", 1).append("name", "test");
    DocumentWithMetadata input = DocumentWithMetadata.of(doc);

    PCollection<DocumentWithMetadata> inputCollection = pipeline.apply(Create.of(input));

    PCollectionTuple output =
        inputCollection.apply(
            "ApplyUDF",
            ParDo.of(
                    new MongoDbTransforms.ApplyUdfFn(
                        udfFile.getAbsolutePath(), "transform", 0, FAILURE_TAG))
                .withOutputTags(MAIN_TAG, TupleTagList.of(FAILURE_TAG)));

    PAssert.that(output.get(MAIN_TAG)).empty();

    PAssert.that(output.get(FAILURE_TAG))
        .satisfies(
            collection -> {
              DocumentWithMetadata result = collection.iterator().next();
              assertTrue(result.getErrorMessage().contains("UDF failed intentionally"));
              return null;
            });

    pipeline.run();
  }

  @Test
  public void applyUdfFn_noopUdf_preservesSpecialDoubles() throws Exception {
    File udfFile = tempFolder.newFile("noop_transform.js");
    try (FileWriter writer = new FileWriter(udfFile)) {
      writer.write(
          "function transform(inJson) {\n"
              + "  var obj = JSON.parse(inJson);\n"
              + "  return JSON.stringify(obj);\n"
              + "}");
    }

    Document doc =
        new Document("_id", 1)
            .append("nanVal", Double.NaN)
            .append("infVal", Double.POSITIVE_INFINITY)
            .append("negInfVal", Double.NEGATIVE_INFINITY);
    DocumentWithMetadata input = DocumentWithMetadata.of(doc);

    PCollection<DocumentWithMetadata> inputCollection = pipeline.apply(Create.of(input));

    PCollectionTuple output =
        inputCollection.apply(
            "ApplyUDF",
            ParDo.of(
                    new MongoDbTransforms.ApplyUdfFn(
                        udfFile.getAbsolutePath(), "transform", 0, FAILURE_TAG))
                .withOutputTags(MAIN_TAG, TupleTagList.of(FAILURE_TAG)));

    PAssert.that(output.get(MAIN_TAG))
        .satisfies(
            collection -> {
              DocumentWithMetadata result = collection.iterator().next();
              Document resultDoc = result.getDocument();
              assertEquals(Double.NaN, resultDoc.get("nanVal"));
              assertEquals(Double.POSITIVE_INFINITY, resultDoc.get("infVal"));
              assertEquals(Double.NEGATIVE_INFINITY, resultDoc.get("negInfVal"));
              return null;
            });

    PAssert.that(output.get(FAILURE_TAG)).empty();

    pipeline.run();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void writeWithDlq_dynamicRouting_writesToCorrectCollection() {
    MongoCollection<Document> col1 = mock(MongoCollection.class);
    MongoCollection<Document> col2 = mock(MongoCollection.class);

    when(staticDatabase.getCollection("col1")).thenReturn(col1);
    when(staticDatabase.getCollection("col2")).thenReturn(col2);

    DocumentWithMetadata doc1 = DocumentWithMetadata.of(new Document("_id", 1), "src1", "col1");
    DocumentWithMetadata doc2 = DocumentWithMetadata.of(new Document("_id", 2), "src2", "col2");

    PCollection<DocumentWithMetadata> input = pipeline.apply(Create.of(doc1, doc2));

    input.apply(
        "Write_Dynamic",
        MongoDbTransforms.writeWithDlq()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withBatchSize(2)
            .withClientFactory(new MockClientFactory()));

    pipeline.run();

    verify(col1).bulkWrite(anyList(), any(BulkWriteOptions.class));
    verify(col2).bulkWrite(anyList(), any(BulkWriteOptions.class));
  }

  @Test
  public void testWriteFn_rateLimitingDisabled() {
    MongoDbTransforms.WriteFn fn =
        MongoDbTransforms.WriteFn.builder()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withInitialWriteRatePerWorker(0)
            .build();
    fn.setup();
    assertNull(fn.getRateLimiter());
    fn.teardown();
  }

  @Test
  public void testWriteFn_linearRampUpRateCalculation() {
    MongoDbTransforms.WriteFn fn =
        MongoDbTransforms.WriteFn.builder()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withInitialWriteRatePerWorker(100)
            .withMaxWriteRatePerWorker(500)
            .withWriteRateRampUpMinutes(5)
            .withWriteRateRampUpSteps(5)
            .build();
    fn.setup();
    assertNotNull(fn.getRateLimiter());
    assertEquals(100.0, fn.getRateLimiter().getRate(), 0.01);

    // Simulate 1 minute elapsed (step 1/5 => 100 + 1 * 80 = 180)
    fn.setStartTimeMs(System.currentTimeMillis() - 1 * 60 * 1000L);
    fn.updateRateLimiterForTest();
    assertEquals(180.0, fn.getRateLimiter().getRate(), 0.01);

    // Simulate 2 minutes elapsed (step 2/5 => 100 + 2 * 80 = 260)
    fn.setStartTimeMs(System.currentTimeMillis() - 2 * 60 * 1000L);
    fn.updateRateLimiterForTest();
    assertEquals(260.0, fn.getRateLimiter().getRate(), 0.01);

    // Simulate 5 minutes elapsed (step 5/5 => 100 + 5 * 80 = 500)
    fn.setStartTimeMs(System.currentTimeMillis() - 5 * 60 * 1000L);
    fn.updateRateLimiterForTest();
    assertEquals(500.0, fn.getRateLimiter().getRate(), 0.01);

    fn.teardown();
  }

  @Test
  public void testWriteFn_defaultAggressiveRampUpCalculation() {
    MongoDbTransforms.WriteFn fn =
        MongoDbTransforms.WriteFn.builder()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .build();
    fn.setup();
    assertNotNull(fn.getRateLimiter());
    assertEquals(5000.0, fn.getRateLimiter().getRate(), 0.01);

    // Simulate 5 minutes elapsed (step 5/5 => 5000 + 5 * 4000 = 25000)
    fn.setStartTimeMs(System.currentTimeMillis() - 5 * 60 * 1000L);
    fn.updateRateLimiterForTest();
    assertEquals(25000.0, fn.getRateLimiter().getRate(), 0.01);

    fn.teardown();
  }

  @Test
  public void testWriteFn_multiBundleExecution_preservesClientAcrossBundles() throws Exception {
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    @SuppressWarnings("unchecked")
    MongoCollection<Document> mockCol = mock(MongoCollection.class);
    when(mockClient.getDatabase(anyString())).thenReturn(mockDb);
    when(mockDb.getCollection(anyString())).thenReturn(mockCol);

    TupleTag<DocumentWithMetadata> failureTag = new TupleTag<>();
    MongoDbTransforms.WriteFn fn =
        MongoDbTransforms.WriteFn.builder()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withBatchSize(1)
            .withMaxWriteRetries(1)
            .withClientFactory(uri -> mockClient)
            .withFailureTag(failureTag)
            .build();

    fn.setup();

    @SuppressWarnings("unchecked")
    DoFn<DocumentWithMetadata, DocumentWithMetadata>.ProcessContext mockCtx =
        mock(DoFn.ProcessContext.class);
    @SuppressWarnings("unchecked")
    DoFn<DocumentWithMetadata, DocumentWithMetadata>.FinishBundleContext mockFinishCtx =
        mock(DoFn.FinishBundleContext.class);

    DocumentWithMetadata doc1 = DocumentWithMetadata.of(new Document("_id", 1), "users", "users");
    when(mockCtx.element()).thenReturn(doc1);

    // Bundle 1
    fn.startBundle();
    fn.processElement(mockCtx);
    fn.finishBundle(mockFinishCtx);

    // Verify client was NOT closed after bundle 1
    org.mockito.Mockito.verify(mockClient, org.mockito.Mockito.never()).close();

    // Bundle 2
    DocumentWithMetadata doc2 = DocumentWithMetadata.of(new Document("_id", 2), "users", "users");
    when(mockCtx.element()).thenReturn(doc2);
    fn.startBundle();
    fn.processElement(mockCtx);
    fn.finishBundle(mockFinishCtx);

    // Verify client still was NOT closed after bundle 2
    org.mockito.Mockito.verify(mockClient, org.mockito.Mockito.never()).close();

    // Teardown closes the client
    fn.teardown();
    org.mockito.Mockito.verify(mockClient, org.mockito.Mockito.times(1)).close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteBatchesCoalescing_insertThenDelete_emitsOnlyDelete() throws Exception {
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    MongoCollection<Document> mockCol = mock(MongoCollection.class);
    when(mockClient.getDatabase(anyString())).thenReturn(mockDb);
    when(mockDb.getCollection(anyString())).thenReturn(mockCol);

    org.mockito.ArgumentCaptor<List<WriteModel<Document>>> captor =
        org.mockito.ArgumentCaptor.forClass(List.class);

    TupleTag<DocumentWithMetadata> failureTag = new TupleTag<>();
    MongoDbTransforms.WriteBatchesFn fn =
        MongoDbTransforms.WriteBatchesFn.builder()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withClientFactory(uri -> mockClient)
            .withFailureTag(failureTag)
            .build();

    fn.setup();
    fn.startBundle();

    Document doc1 = new Document("_id", 100).append("name", "Alice");
    DocumentWithMetadata insertItem = DocumentWithMetadata.of(doc1, "users", "users");

    DocumentWithMetadata deleteItem =
        DocumentWithMetadata.cdcEvent(
            null,
            null,
            "users",
            "users",
            DocumentWithMetadata.OperationType.DELETE,
            TimestampSortKey.cdc(1000, 1),
            new Document("_id", 100).toJson());

    DoFn<KV<String, Iterable<DocumentWithMetadata>>, DocumentWithMetadata>.ProcessContext mockCtx =
        mock(DoFn.ProcessContext.class);
    when(mockCtx.element()).thenReturn(KV.of("users#0", Arrays.asList(insertItem, deleteItem)));

    fn.processElement(mockCtx);

    @SuppressWarnings("unchecked")
    DoFn<KV<String, Iterable<DocumentWithMetadata>>, DocumentWithMetadata>.FinishBundleContext
        mockFinishCtx = mock(DoFn.FinishBundleContext.class);
    fn.finishBundle(mockFinishCtx);
    fn.teardown();

    verify(mockCol).bulkWrite(captor.capture(), any(BulkWriteOptions.class));
    List<WriteModel<Document>> capturedModels = captor.getValue();
    assertEquals(1, capturedModels.size());
    assertTrue(capturedModels.get(0) instanceof DeleteOneModel);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteBatchesCoalescing_multipleUpdates_emitsLatestPayload() throws Exception {
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    MongoCollection<Document> mockCol = mock(MongoCollection.class);
    when(mockClient.getDatabase(anyString())).thenReturn(mockDb);
    when(mockDb.getCollection(anyString())).thenReturn(mockCol);

    org.mockito.ArgumentCaptor<List<WriteModel<Document>>> captor =
        org.mockito.ArgumentCaptor.forClass(List.class);

    TupleTag<DocumentWithMetadata> failureTag = new TupleTag<>();
    MongoDbTransforms.WriteBatchesFn fn =
        MongoDbTransforms.WriteBatchesFn.builder()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withClientFactory(uri -> mockClient)
            .withFailureTag(failureTag)
            .build();

    fn.setup();
    fn.startBundle();

    Document docV1 = new Document("_id", 200).append("val", 1);
    Document docV2 = new Document("_id", 200).append("val", 2);
    Document docV3 = new Document("_id", 200).append("val", 3);

    DocumentWithMetadata item1 = DocumentWithMetadata.of(docV1, "metrics", "metrics");
    DocumentWithMetadata item2 = DocumentWithMetadata.of(docV2, "metrics", "metrics");
    DocumentWithMetadata item3 = DocumentWithMetadata.of(docV3, "metrics", "metrics");

    DoFn<KV<String, Iterable<DocumentWithMetadata>>, DocumentWithMetadata>.ProcessContext mockCtx =
        mock(DoFn.ProcessContext.class);
    when(mockCtx.element()).thenReturn(KV.of("metrics#1", Arrays.asList(item1, item2, item3)));

    fn.processElement(mockCtx);

    @SuppressWarnings("unchecked")
    DoFn<KV<String, Iterable<DocumentWithMetadata>>, DocumentWithMetadata>.FinishBundleContext
        mockFinishCtx = mock(DoFn.FinishBundleContext.class);
    fn.finishBundle(mockFinishCtx);
    fn.teardown();

    verify(mockCol).bulkWrite(captor.capture(), any(BulkWriteOptions.class));
    List<WriteModel<Document>> capturedModels = captor.getValue();
    assertEquals(1, capturedModels.size());
    assertTrue(capturedModels.get(0) instanceof ReplaceOneModel);
    ReplaceOneModel<Document> replaceModel = (ReplaceOneModel<Document>) capturedModels.get(0);
    assertEquals(3, replaceModel.getReplacement().get("val"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteBatchesCoalescing_mixedUniqueAndDuplicates() throws Exception {
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    MongoCollection<Document> mockCol = mock(MongoCollection.class);
    when(mockClient.getDatabase(anyString())).thenReturn(mockDb);
    when(mockDb.getCollection(anyString())).thenReturn(mockCol);

    org.mockito.ArgumentCaptor<List<WriteModel<Document>>> captor =
        org.mockito.ArgumentCaptor.forClass(List.class);

    TupleTag<DocumentWithMetadata> failureTag = new TupleTag<>();
    MongoDbTransforms.WriteBatchesFn fn =
        MongoDbTransforms.WriteBatchesFn.builder()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withClientFactory(uri -> mockClient)
            .withFailureTag(failureTag)
            .build();

    fn.setup();
    fn.startBundle();

    List<DocumentWithMetadata> batchItems = new ArrayList<>();
    // 5 unique items
    for (int i = 1; i <= 5; i++) {
      batchItems.add(
          DocumentWithMetadata.of(new Document("_id", i).append("name", "name" + i), "items", "items"));
    }
    // Update to id=1 and id=2
    batchItems.add(
        DocumentWithMetadata.of(
            new Document("_id", 1).append("name", "name1_updated"), "items", "items"));
    batchItems.add(
        DocumentWithMetadata.of(
            new Document("_id", 2).append("name", "name2_updated"), "items", "items"));
    // Delete for id=3
    batchItems.add(
        DocumentWithMetadata.cdcEvent(
            null,
            null,
            "items",
            "items",
            DocumentWithMetadata.OperationType.DELETE,
            TimestampSortKey.cdc(2000, 1),
            new Document("_id", 3).toJson()));

    DoFn<KV<String, Iterable<DocumentWithMetadata>>, DocumentWithMetadata>.ProcessContext mockCtx =
        mock(DoFn.ProcessContext.class);
    when(mockCtx.element()).thenReturn(KV.of("items#0", batchItems));

    fn.processElement(mockCtx);

    @SuppressWarnings("unchecked")
    DoFn<KV<String, Iterable<DocumentWithMetadata>>, DocumentWithMetadata>.FinishBundleContext
        mockFinishCtx = mock(DoFn.FinishBundleContext.class);
    fn.finishBundle(mockFinishCtx);
    fn.teardown();

    verify(mockCol).bulkWrite(captor.capture(), any(BulkWriteOptions.class));
    List<WriteModel<Document>> capturedModels = captor.getValue();
    assertEquals(5, capturedModels.size());
  }
}
