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
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
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
  public void testWriteWithDlq_TransientError_Retries() {
    // Simulate transient error (code 11600 - Interrupted) on first call, then success.
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
        MongoDbTransforms.writeWithDlq()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withCollection("test")
            .withMaxRetries(3)
            .withBatchSize(1)
            .withClientFactory(new MockClientFactory()));

    pipeline.run();

    // Verify that bulkWrite was called twice (one retry).
    assertEquals(2, callCount.get());
  }

  @Test
  public void testWriteWithDlq_PermanentError_NoRetry() {
    // Simulate permanent error (code 11000 - Duplicate Key).
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
        MongoDbTransforms.writeWithDlq()
            .withUri("mongodb://localhost:27017")
            .withDatabase("test")
            .withCollection("test")
            .withMaxRetries(3)
            .withBatchSize(1)
            .withClientFactory(new MockClientFactory()));

    pipeline.run();

    // Verify that bulkWrite was called only once (no retry).
    assertEquals(1, callCount.get());
  }

  // Static class to avoid capturing outer instance
  private static class MockClientFactory implements SerializableFunction<String, MongoClient> {
    @Override
    public MongoClient apply(String input) {
      return staticClient;
    }
  }
}
