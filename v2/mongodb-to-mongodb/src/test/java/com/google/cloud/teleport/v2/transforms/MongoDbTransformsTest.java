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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
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
  public void writeWithDlq_documentLevelRetry_partialSuccess()
      throws org.apache.beam.sdk.coders.CannotProvideCoderException {
    AtomicInteger callCount = new AtomicInteger(0);
    when(staticCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenAnswer(
            invocation -> {
              int count = callCount.getAndIncrement();
              if (count == 0) {
                throw new MongoBulkWriteException(
                    mock(BulkWriteResult.class),
                    Arrays.asList(
                        new BulkWriteError(11000, "Duplicate Key", new BsonDocument(), 1),
                        new BulkWriteError(11600, "Interrupted", new BsonDocument(), 2)),
                    null,
                    new ServerAddress(),
                    Collections.emptySet());
              } else if (count == 1) {
                List<WriteModel<Document>> updates = invocation.getArgument(0);
                assertEquals(1, updates.size());
                return mock(BulkWriteResult.class);
              }
              return mock(BulkWriteResult.class);
            });

    DocumentWithMetadata doc0 = DocumentWithMetadata.of(new Document("_id", 0), "test", "test");
    DocumentWithMetadata doc1 = DocumentWithMetadata.of(new Document("_id", 1), "test", "test");
    DocumentWithMetadata doc2 = DocumentWithMetadata.of(new Document("_id", 2), "test", "test");

    KV<String, Iterable<DocumentWithMetadata>> batch =
        KV.of("fixed-key", Arrays.asList(doc0, doc1, doc2));

    Coder<DocumentWithMetadata> documentWithMetadataCoder =
        pipeline.getCoderRegistry().getCoder(TypeDescriptor.of(DocumentWithMetadata.class));

    PCollection<KV<String, Iterable<DocumentWithMetadata>>> input =
        pipeline.apply(
            Create.of(Collections.singletonList(batch))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(documentWithMetadataCoder))));

    input.apply(
        "Write_DocLevelRetry",
        ParDo.of(
                MongoDbTransforms.WriteFn.builder()
                    .withUri("mongodb://localhost:27017")
                    .withDatabase("test")
                    .withMaxWriteRetries(3)
                    .withMaxConcurrentAsyncWrites(1)
                    .withClientFactory(new MockClientFactory())
                    .withFailureTag(FAILURE_TAG)
                    .build())
            .withOutputTags(MAIN_TAG, TupleTagList.of(FAILURE_TAG)));

    PipelineResult result = pipeline.run();

    assertEquals(2, callCount.get());
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
              org.junit.Assert.assertTrue(
                  result.getErrorMessage().contains("UDF failed intentionally"));
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

    org.mockito.Mockito.verify(col1).bulkWrite(anyList(), any(BulkWriteOptions.class));
    org.mockito.Mockito.verify(col2).bulkWrite(anyList(), any(BulkWriteOptions.class));
  }
}
