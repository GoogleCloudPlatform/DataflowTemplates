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
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MongoDbTransformsTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  // Static fields to avoid serialization issues in Beam tests
  private static MongoClient staticClient;
  private static MongoDatabase staticDatabase;
  private static MongoCollection<Document> staticCollection;

  @Before
  public void setUp() {
    staticClient = mock(MongoClient.class);
    staticDatabase = mock(MongoDatabase.class);
    staticCollection = mock(MongoCollection.class);

    when(staticClient.getDatabase(anyString())).thenReturn(staticDatabase);
    when(staticDatabase.getCollection(anyString())).thenReturn(staticCollection);
  }

  @Test
  public void test1_WriteWithDlq_TransientError_Retries() {
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

    PCollection<Document> input = pipeline.apply(Create.<Document>of(new Document("_id", 1)));

    input.apply(
        "Write_Transient",
        MongoDbTransforms.writeWithDlq()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withCollection("test")
            .withMaxRetries(3)
            .withBatchSize(1)
            .withClientFactory(new MockClientFactory()));

    PipelineResult result = pipeline.run();

    assertEquals(2, callCount.get());

    long successCount = 0;
    for (MetricResult<Long> c : result.metrics().queryMetrics(MetricsFilter.builder().build()).getCounters()) {
      if (c.getName().getName().equals("successful-documents-written")) {
        successCount = c.getCommitted();
      }
    }
    assertEquals(1L, successCount);
  }

  @Test
  public void test2_WriteWithDlq_PermanentError_NoRetry() {
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

    PCollection<Document> input = pipeline.apply(Create.<Document>of(new Document("_id", 1)));

    input.apply(
        "Write_Permanent",
        MongoDbTransforms.writeWithDlq()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withCollection("test")
            .withMaxRetries(3)
            .withBatchSize(1)
            .withClientFactory(new MockClientFactory()));

    PipelineResult result = pipeline.run();

    assertEquals(1, callCount.get());

    long successCount = 0;
    for (MetricResult<Long> c : result.metrics().queryMetrics(MetricsFilter.builder().build()).getCounters()) {
      if (c.getName().getName().equals("successful-documents-written")) {
        successCount = c.getCommitted();
      }
    }
    assertEquals(0L, successCount); // Reverted to 0
  }

  @Test
  public void test3_WriteWithDlq_UnorderedPartialSuccess() {
    AtomicInteger callCount = new AtomicInteger(0);
    when(staticCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
        .thenAnswer(
            invocation -> {
              if (callCount.getAndIncrement() == 0) {
                throw new MongoBulkWriteException(
                    mock(BulkWriteResult.class),
                    Arrays.asList(new BulkWriteError(11000, "Duplicate Key", new BsonDocument(), 0)),
                    null,
                    new ServerAddress(),
                    Collections.emptySet());
              }
              return mock(BulkWriteResult.class);
            });

    PCollection<Document> input = pipeline.apply(Create.<Document>of(
        new Document("_id", 1),
        new Document("_id", 2)
    ));

    input.apply(
        "Write_Partial",
        MongoDbTransforms.writeWithDlq()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withCollection("test")
            .withMaxRetries(3)
            .withBatchSize(2)
            .withOrdered(false)
            .withClientFactory(new MockClientFactory()));

    PipelineResult result = pipeline.run();

    assertEquals(2, callCount.get());

    long successCount = 0;
    for (MetricResult<Long> c : result.metrics().queryMetrics(MetricsFilter.builder().build()).getCounters()) {
      if (c.getName().getName().equals("successful-documents-written")) {
        successCount = c.getCommitted();
      }
    }
    assertEquals(1L, successCount); // Reverted to 1
  }

  private static class MockClientFactory implements SerializableFunction<String, MongoClient> {
    @Override
    public MongoClient apply(String input) {
      return staticClient;
    }
  }
}
