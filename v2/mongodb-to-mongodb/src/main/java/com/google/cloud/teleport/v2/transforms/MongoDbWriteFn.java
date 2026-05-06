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

import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.bson.Document;
import org.joda.time.Instant;

/** A {@link DoFn} that writes documents to MongoDB in bulk. */
public class MongoDbWriteFn extends DoFn<KV<String, Iterable<Document>>, Document> {

  private static final int ERR_DOCUMENT_VALIDATION_FAILURE = 121;
  private static final int ERR_KEY_TOO_LONG = 17280;
  private static final int ERR_BAD_VALUE = 2;

  private final String uri;
  private final String database;
  private final String collection;
  private final Integer maxConcurrentAsyncWrites;
  private final Integer maxRetries;
  private final SerializableFunction<String, MongoClient> clientFactory;
  private final TupleTag<String> failureTag;

  private final Counter successfulDocs =
      Metrics.counter(MongoDbTransforms.WriteWithDlq.class, "successful-documents-written");

  private transient MongoClient mongoClient;
  private transient MongoCollection<Document> mongoCollection;
  private transient ExecutorService executor;
  private transient Semaphore semaphore;
  private transient ConcurrentLinkedQueue<CompletableFuture<Void>> futures;
  private transient ConcurrentLinkedQueue<String> failures;
  private transient AtomicLong successfulCount;

  public MongoDbWriteFn(
      String uri,
      String database,
      String collection,
          Integer maxConcurrentAsyncWrites,
      Integer maxRetries,
      SerializableFunction<String, MongoClient> clientFactory,
      TupleTag<String> failureTag) {
    this.uri = uri;
    this.database = database;
    this.collection = collection;
    this.maxConcurrentAsyncWrites = maxConcurrentAsyncWrites;
    this.maxRetries = maxRetries;
    this.clientFactory = clientFactory;
    this.failureTag = failureTag;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String uri;
    private String database;
    private String collection;
    private Integer maxConcurrentAsyncWrites;
    private Integer maxRetries;
    private SerializableFunction<String, MongoClient> clientFactory;
    private TupleTag<String> failureTag;

    public Builder withUri(String uri) {
      this.uri = uri;
      return this;
    }

    public Builder withDatabase(String database) {
      this.database = database;
      return this;
    }

    public Builder withCollection(String collection) {
      this.collection = collection;
      return this;
    }


    public Builder withMaxConcurrentAsyncWrites(Integer maxConcurrentAsyncWrites) {
      this.maxConcurrentAsyncWrites = maxConcurrentAsyncWrites;
      return this;
    }

    public Builder withMaxRetries(Integer maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    public Builder withClientFactory(SerializableFunction<String, MongoClient> clientFactory) {
      this.clientFactory = clientFactory;
      return this;
    }

    public Builder withFailureTag(TupleTag<String> failureTag) {
      this.failureTag = failureTag;
      return this;
    }

    public MongoDbWriteFn build() {
      return new MongoDbWriteFn(
          uri,
          database,
          collection,
              maxConcurrentAsyncWrites,
          maxRetries,
          clientFactory,
          failureTag);
    }
  }

  @Setup
  public void setup() {
    executor = Executors.newFixedThreadPool(maxConcurrentAsyncWrites);
    semaphore = new Semaphore(maxConcurrentAsyncWrites);
  }

  @Teardown
  public void teardown() {
    if (executor != null) {
      executor.shutdown();
    }
  }

  @StartBundle
  public void startBundle() {
    mongoClient = clientFactory.apply(uri);
    MongoDatabase db = mongoClient.getDatabase(database);

    mongoCollection = db.getCollection(collection);

    futures = new ConcurrentLinkedQueue<>();
    failures = new ConcurrentLinkedQueue<>();
    successfulCount = new AtomicLong(0);
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws InterruptedException {
    if (mongoCollection == null) {
      mongoCollection = clientFactory.apply(uri).getDatabase(database).getCollection(collection);
    }
    Iterable<Document> docs = c.element().getValue();
    List<WriteModel<Document>> updates = new ArrayList<>();
    List<Document> docList = new ArrayList<>();

    for (Document doc : docs) {
      docList.add(doc);
      Object id = doc.get("_id");
      if (id != null) {
        updates.add(
            new ReplaceOneModel<>(new Document("_id", id), doc, new ReplaceOptions().upsert(true)));
      }
    }

    if (!updates.isEmpty()) {
      semaphore.acquire();
      CompletableFuture<Void> future =
          CompletableFuture.runAsync(
              () -> {
                try {
                  int attempt = 0;
                  boolean success = false;
                  while (attempt < maxRetries && !success) {
                    try {
                      mongoCollection.bulkWrite(updates, new BulkWriteOptions().ordered(false));
                      success = true;
                      successfulCount.addAndGet(docList.size());
                    } catch (MongoBulkWriteException e) {

                      List<BulkWriteError> writeErrors = e.getWriteErrors();
                      boolean hasPermanentError = false;
                      for (BulkWriteError error : writeErrors) {
                        int code = error.getCode();
                        if (ErrorCategory.fromErrorCode(code) == ErrorCategory.DUPLICATE_KEY
                            || code == ERR_DOCUMENT_VALIDATION_FAILURE
                            || code == ERR_KEY_TOO_LONG
                            || code == ERR_BAD_VALUE) {
                          hasPermanentError = true;
                          break;
                        }
                      }

                      if (hasPermanentError || attempt + 1 >= maxRetries) {
                        successfulCount.addAndGet(docList.size() - writeErrors.size());

                        for (BulkWriteError error : writeErrors) {
                          int index = error.getIndex();
                          if (index >= 0 && index < docList.size()) {
                            Document failedDoc = docList.get(index);
                            failures.add(failedDoc.toJson() + " - Error: " + error.getMessage());
                          }
                        }
                        break;
                      }

                      attempt++;
                      long delay = (long) (1000 * Math.pow(2, attempt));
                      try {
                        Thread.sleep(delay);
                      } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                      }
                    } catch (MongoException e) {

                      int code = e.getCode();
                      boolean isPermanent =
                          (ErrorCategory.fromErrorCode(code) == ErrorCategory.DUPLICATE_KEY
                              || code == ERR_DOCUMENT_VALIDATION_FAILURE
                              || code == ERR_KEY_TOO_LONG
                              || code == ERR_BAD_VALUE);

                      if (isPermanent || attempt + 1 >= maxRetries) {
                        for (Document doc : docList) {
                          failures.add(doc.toJson() + " - Error: " + e.getMessage());
                        }
                        break;
                      }

                      attempt++;
                      long delay = (long) (1000 * Math.pow(2, attempt));
                      try {
                        Thread.sleep(delay);
                      } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                      }
                    } catch (Exception e) {

                      for (Document doc : docList) {
                        failures.add(doc.toJson() + " - Error: " + e.getMessage());
                      }
                      break;
                    }
                  }
                } finally {
                  semaphore.release();
                }
              },
              executor);
      futures.add(future);
    }
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext c) {
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    if (mongoClient != null) {
      mongoClient.close();
    }

    successfulDocs.inc(successfulCount.get());

    String failure;
    while ((failure = failures.poll()) != null) {
      c.output(failureTag, failure, Instant.now(), GlobalWindow.INSTANCE);
    }
  }
}
