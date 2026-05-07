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

import static com.google.cloud.teleport.v2.transforms.DocumentWithMetadata.ErrorType.PERMANENT;
import static com.google.cloud.teleport.v2.transforms.DocumentWithMetadata.ErrorType.RETRYABLE;

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
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.bson.Document;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link DoFn} that writes documents to MongoDB in bulk. */
public class MongoDbWriteFn
    extends DoFn<KV<String, Iterable<DocumentWithMetadata>>, DocumentWithMetadata> {

  private static final int ERR_DOCUMENT_VALIDATION_FAILURE = 121;
  private static final int ERR_KEY_TOO_LONG = 17280;
  private static final int ERR_BAD_VALUE = 2;

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbWriteFn.class);

  private final String uri;
  private final String database;
  private final String collection;
  private final Integer maxConcurrentAsyncWrites;
  private final Integer maxWriteRetries;
  private final Integer dlqMaxRetries;
  private final SerializableFunction<String, MongoClient> clientFactory;
  private final TupleTag<String> failureTag;
  private transient FluentBackoff backoffSpec;

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
      Integer maxWriteRetries,
      Integer dlqMaxRetries,
      SerializableFunction<String, MongoClient> clientFactory,
      TupleTag<String> failureTag) {
    this.uri = uri;
    this.database = database;
    this.collection = collection;
    this.maxConcurrentAsyncWrites = maxConcurrentAsyncWrites;
    this.maxWriteRetries = maxWriteRetries;
    this.dlqMaxRetries = dlqMaxRetries;
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
    private Integer maxWriteRetries;
    private Integer dlqMaxRetries;
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

    public Builder withMaxWriteRetries(Integer maxWriteRetries) {
      this.maxWriteRetries = maxWriteRetries;
      return this;
    }

    public Builder withDlqMaxRetries(Integer dlqMaxRetries) {
      this.dlqMaxRetries = dlqMaxRetries;
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
          maxWriteRetries,
          dlqMaxRetries,
          clientFactory,
          failureTag);
    }
  }

  @Setup
  public void setup() {
    executor = Executors.newFixedThreadPool(maxConcurrentAsyncWrites);
    semaphore = new Semaphore(maxConcurrentAsyncWrites);
    backoffSpec =
        FluentBackoff.DEFAULT
            .withMaxRetries(maxWriteRetries)
            .withInitialBackoff(Duration.standardSeconds(2))
            .withExponent(2.0);
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
    Iterable<DocumentWithMetadata> items = c.element().getValue();
    List<WriteModel<Document>> updates = new ArrayList<>();
    List<DocumentWithMetadata> itemList = new ArrayList<>();

    for (DocumentWithMetadata item : items) {
      itemList.add(item);
      Document doc = item.getDocument();
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
                  List<WriteModel<Document>> currentUpdates = updates;
                  List<DocumentWithMetadata> currentItemList = itemList;
                  BackOff backoff = backoffSpec.backoff();
                  Sleeper sleeper = Sleeper.DEFAULT;

                  while (true) {
                    try {
                      mongoCollection.bulkWrite(
                          currentUpdates, new BulkWriteOptions().ordered(false));
                      successfulCount.addAndGet(currentItemList.size());
                      break;
                    } catch (MongoBulkWriteException e) {
                      // MongoBulkWriteException provides a list of write errors, one for each write
                      // operation that failed. For this exception type we only retry the failed
                      // cases.
                      List<BulkWriteError> writeErrors = e.getWriteErrors();
                      successfulCount.addAndGet(currentItemList.size() - writeErrors.size());

                      List<WriteModel<Document>> nextUpdates = new ArrayList<>();
                      List<DocumentWithMetadata> nextItemList = new ArrayList<>();
                      generateRetryBatch(
                          writeErrors, currentUpdates, currentItemList, nextUpdates, nextItemList);

                      if (nextUpdates.isEmpty()) {
                        break;
                      }

                      if (handleBackoff(sleeper, backoff, nextItemList)) {
                        break;
                      }

                      currentUpdates = nextUpdates;
                      currentItemList = nextItemList;
                    } catch (Exception e) {

                      // Permanent errors are logged and sent to the DLQ immediately .
                      if (!isRetriable(e)) {
                        writePermanentDlqMessage(
                            currentItemList, "Failed to write documents: " + e.getMessage());
                        break;
                      }

                      // Retryable errors are retried using exponential backoff. If the backoff is
                      // exhausted, the error is logged and sent to the DLQ.
                      if (handleBackoff(sleeper, backoff, currentItemList)) {
                        break;
                      }
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

  private boolean isPermanentErrorCode(int code) {
    return ErrorCategory.fromErrorCode(code) == ErrorCategory.DUPLICATE_KEY
        || code == ERR_DOCUMENT_VALIDATION_FAILURE
        || code == ERR_KEY_TOO_LONG
        || code == ERR_BAD_VALUE;
  }

  private boolean isRetriable(Exception e) {
    if (e instanceof MongoException me) {
      return !isPermanentErrorCode(me.getCode());
    }
    return false;
  }

  private void generateRetryBatch(
      List<BulkWriteError> writeErrors,
      List<WriteModel<Document>> currentUpdates,
      List<DocumentWithMetadata> currentItemList,
      List<WriteModel<Document>> nextUpdates,
      List<DocumentWithMetadata> nextItemList) {
    for (BulkWriteError error : writeErrors) {
      int index = error.getIndex();
      if (index >= 0 && index < currentItemList.size()) {
        DocumentWithMetadata failedItem = currentItemList.get(index);
        WriteModel<Document> failedUpdate = currentUpdates.get(index);

        if (isPermanentErrorCode(error.getCode())) {
          writePermanentDlqMessage(
              java.util.Collections.singletonList(failedItem),
              "Permanent failure writing document. Error: " + error.getMessage());
        } else {
          nextUpdates.add(failedUpdate);
          nextItemList.add(failedItem);
        }
      }
    }
  }

  private void writePermanentDlqMessage(List<DocumentWithMetadata> itemList, String message) {
    writeToDlq(itemList, message, true);
  }

  private void writeRetryableDlqMessage(List<DocumentWithMetadata> itemList, String message) {
    writeToDlq(itemList, message, false);
  }

  private void writeToDlq(
      List<DocumentWithMetadata> itemList, String message, boolean isPermanent) {
    for (DocumentWithMetadata item : itemList) {
      String docStr =
          item.getOriginalDocument() != null
              ? item.getOriginalDocument()
              : item.getDocument().toJson();
      LOG.warn("{}: {}", message, docStr);
      try {
        int retryCount = isPermanent ? dlqMaxRetries + 1 : item.getRetryCount() + 1;
        DocumentWithMetadata.ErrorType errorType = isPermanent ? PERMANENT : RETRYABLE;
        failures.add(item.toDlqJson(message, errorType, retryCount));
      } catch (Exception e) {
        LOG.error("Failed to format DLQ message", e);
        // Fallback to simple JSON if parsing fails
        failures.add(
            String.format(
                "{\"message\": {\"data\": %s}, \"error_message\": \"%s\"}", docStr, message));
      }
    }
  }

  private boolean handleBackoff(
      Sleeper sleeper, BackOff backoff, List<DocumentWithMetadata> itemList) {
    try {
      if (!BackOffUtils.next(sleeper, backoff)) {
        writeRetryableDlqMessage(itemList, "Backoff exhausted. Failed to write documents");
        return true;
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      writeRetryableDlqMessage(itemList, "Interrupted while writing documents");
      return true;
    }
    return false;
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
