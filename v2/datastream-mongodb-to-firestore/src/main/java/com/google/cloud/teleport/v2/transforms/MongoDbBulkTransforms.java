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

import static com.mongodb.client.model.Filters.eq;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import com.mongodb.ConnectionString;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.conversions.Bson;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** High-throughput asynchronous bulk transforms for writing CDC events to Firestore/MongoDB. */
public class MongoDbBulkTransforms {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbBulkTransforms.class);

  // Permanent error codes that should not be retried in-memory
  public static final int ERR_BAD_VALUE = 2;
  public static final int ERR_UNAUTHORIZED = 13;
  public static final int ERR_TYPE_MISMATCH = 14;
  public static final int ERR_INVALID_LENGTH = 16;
  public static final int ERR_NAMESPACE_NOT_FOUND = 26;
  public static final int ERR_IMMUTABLE_FIELD = 66;
  public static final int ERR_NETWORK_TIMEOUT = 89;
  public static final int ERR_SHUTDOWN_IN_PROGRESS = 91;
  public static final int ERR_WRITE_CONFLICT = 112;
  public static final int ERR_DOCUMENT_VALIDATION_FAILURE = 121;
  public static final int ERR_DUPLICATE_KEY = 11000;
  public static final int ERR_KEY_TOO_LONG = 17280;

  public static final Set<Integer> PERMANENT_ERROR_CODES =
      ImmutableSet.of(
          ERR_BAD_VALUE,
          ERR_UNAUTHORIZED,
          ERR_TYPE_MISMATCH,
          ERR_INVALID_LENGTH,
          ERR_NAMESPACE_NOT_FOUND,
          ERR_IMMUTABLE_FIELD,
          ERR_DOCUMENT_VALIDATION_FAILURE,
          ERR_KEY_TOO_LONG);

  public static final TupleTag<MongoDbChangeEventContext> SUCCESSFUL_WRITE_TAG =
      new TupleTag<MongoDbChangeEventContext>("successfulWrite") {};
  public static final TupleTag<
          FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
      FAILED_WRITE_TAG =
          new TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>(
              "failedWrite") {};
  public static final TupleTag<
          FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
      SEVERE_FAILED_WRITE_TAG =
          new TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>(
              "severeFailedWrite") {};

  public static MongoClient createMongoClient(String uri) {
    ConnectionString connectionString = new ConnectionString(uri);
    MongoClientSettings.Builder builder =
        MongoClientSettings.builder().applyConnectionString(connectionString);
    if (connectionString.getUuidRepresentation() == null
        || connectionString.getUuidRepresentation() == UuidRepresentation.UNSPECIFIED) {
      builder.uuidRepresentation(UuidRepresentation.STANDARD);
    }
    builder.applyToSocketSettings(
        b -> {
          b.connectTimeout(60, TimeUnit.SECONDS);
          b.readTimeout(60, TimeUnit.SECONDS);
        });
    builder.applyToConnectionPoolSettings(
        b -> {
          b.minSize(10);
          b.maxSize(50);
          b.maxWaitTime(5, TimeUnit.SECONDS);
        });
    builder.applyToClusterSettings(b -> b.serverSelectionTimeout(10, TimeUnit.MINUTES));
    return MongoClients.create(builder.build());
  }

  public static boolean isPermanentErrorCode(int code) {
    return PERMANENT_ERROR_CODES.contains(code);
  }

  public static boolean isPermanentError(int code) {
    return isPermanentErrorCode(code);
  }

  public static BulkWriteWithDlq bulkWriteWithDlq() {
    return new BulkWriteWithDlq();
  }

  /** PTransform encapsulating asynchronous bulk writing with retry and severe DLQ routing. */
  public static class BulkWriteWithDlq
      extends PTransform<PCollection<MongoDbChangeEventContext>, PCollectionTuple> {

    public static final TupleTag<MongoDbChangeEventContext> SUCCESS_TAG = SUCCESSFUL_WRITE_TAG;
    public static final TupleTag<
            FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
        FAILED_TAG = FAILED_WRITE_TAG;
    public static final TupleTag<
            FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
        SEVERE_FAILED_TAG = SEVERE_FAILED_WRITE_TAG;

    private String connectionString;
    private String database;
    private int batchSize = 500;
    private int maxConcurrentAsyncWrites = 10;
    private int initialWriteRatePerWorker = 500;
    private int writeRateRampUpMinutes = 5;
    private int writeRateRampUpSteps = 5;
    private int maxWriteRatePerWorker = 2500;
    private int maxWriteRetries = 3;
    private int dlqMaxRetries = 3;
    private SerializableFunction<String, MongoClient> clientFactory =
        MongoDbBulkTransforms::createMongoClient;

    public BulkWriteWithDlq withConnectionString(String connectionString) {
      this.connectionString = connectionString;
      return this;
    }

    public BulkWriteWithDlq withUri(String uri) {
      this.connectionString = uri;
      return this;
    }

    public BulkWriteWithDlq withDatabase(String database) {
      this.database = database;
      return this;
    }

    public BulkWriteWithDlq withBatchSize(Integer batchSize) {
      if (batchSize != null) {
        this.batchSize = batchSize;
      }
      return this;
    }

    public BulkWriteWithDlq withMaxConcurrentAsyncWrites(Integer maxConcurrentAsyncWrites) {
      if (maxConcurrentAsyncWrites != null) {
        this.maxConcurrentAsyncWrites = maxConcurrentAsyncWrites;
      }
      return this;
    }

    public BulkWriteWithDlq withInitialWriteRatePerWorker(Integer initialWriteRatePerWorker) {
      if (initialWriteRatePerWorker != null) {
        this.initialWriteRatePerWorker = initialWriteRatePerWorker;
      }
      return this;
    }

    public BulkWriteWithDlq withWriteRateRampUpMinutes(Integer writeRateRampUpMinutes) {
      if (writeRateRampUpMinutes != null) {
        this.writeRateRampUpMinutes = writeRateRampUpMinutes;
      }
      return this;
    }

    public BulkWriteWithDlq withWriteRateRampUpSteps(Integer writeRateRampUpSteps) {
      if (writeRateRampUpSteps != null) {
        this.writeRateRampUpSteps = writeRateRampUpSteps;
      }
      return this;
    }

    public BulkWriteWithDlq withMaxWriteRatePerWorker(Integer maxWriteRatePerWorker) {
      if (maxWriteRatePerWorker != null) {
        this.maxWriteRatePerWorker = maxWriteRatePerWorker;
      }
      return this;
    }

    public BulkWriteWithDlq withMaxWriteRetries(Integer maxWriteRetries) {
      if (maxWriteRetries != null) {
        this.maxWriteRetries = maxWriteRetries;
      }
      return this;
    }

    public BulkWriteWithDlq withDlqMaxRetries(Integer dlqMaxRetries) {
      if (dlqMaxRetries != null) {
        this.dlqMaxRetries = dlqMaxRetries;
      }
      return this;
    }

    public BulkWriteWithDlq withClientFactory(
        SerializableFunction<String, MongoClient> clientFactory) {
      if (clientFactory != null) {
        this.clientFactory = clientFactory;
      }
      return this;
    }

    @Override
    public PCollectionTuple expand(PCollection<MongoDbChangeEventContext> input) {
      PCollectionTuple result =
          input.apply(
              "AsyncBulkWriteFn",
              ParDo.of(
                      new BulkWriteFn(
                          connectionString,
                          database,
                          batchSize,
                          maxConcurrentAsyncWrites,
                          initialWriteRatePerWorker,
                          writeRateRampUpMinutes,
                          writeRateRampUpSteps,
                          maxWriteRatePerWorker,
                          maxWriteRetries,
                          dlqMaxRetries,
                          clientFactory,
                          SUCCESS_TAG,
                          FAILED_TAG,
                          SEVERE_FAILED_TAG))
                  .withOutputTags(SUCCESS_TAG, TupleTagList.of(FAILED_TAG).and(SEVERE_FAILED_TAG)));

      result.get(SUCCESS_TAG).setCoder(SerializableCoder.of(MongoDbChangeEventContext.class));
      result
          .get(FAILED_TAG)
          .setCoder(
              FailsafeElementCoder.of(
                  SerializableCoder.of(MongoDbChangeEventContext.class),
                  SerializableCoder.of(MongoDbChangeEventContext.class)));
      result
          .get(SEVERE_FAILED_TAG)
          .setCoder(
              FailsafeElementCoder.of(
                  SerializableCoder.of(MongoDbChangeEventContext.class),
                  SerializableCoder.of(MongoDbChangeEventContext.class)));

      return result;
    }
  }

  /** DoFn implementing non-transactional async bulk write with rate limiter ramp-up and triage. */
  public static class BulkWriteFn
      extends DoFn<MongoDbChangeEventContext, MongoDbChangeEventContext> {

    private static final Logger LOG = LoggerFactory.getLogger(BulkWriteFn.class);

    private final String connectionString;
    private final String database;
    private final int batchSize;
    private final int maxConcurrentAsyncWrites;
    private final int initialWriteRatePerWorker;
    private final int writeRateRampUpMinutes;
    private final int writeRateRampUpSteps;
    private final int maxWriteRatePerWorker;
    private final int maxWriteRetries;
    private final int dlqMaxRetries;
    private final SerializableFunction<String, MongoClient> clientFactory;
    private final TupleTag<MongoDbChangeEventContext> successTag;
    private final TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
        failureTag;
    private final TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
        severeFailureTag;

    private transient MongoClient mongoClient;
    private transient ExecutorService executorService;
    private transient Semaphore semaphore;
    private transient RateLimiter rateLimiter;
    private transient long setupStartTimeMs;
    private transient Map<String, List<MongoDbChangeEventContext>> currentBatches;
    private transient List<CompletableFuture<Void>> inFlightFutures;
    private transient ConcurrentLinkedQueue<MongoDbChangeEventContext> successQueue;
    private transient ConcurrentLinkedQueue<
            FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
        failureQueue;
    private transient ConcurrentLinkedQueue<
            FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
        severeFailureQueue;
    private transient Map<String, MongoCollection<Document>> collectionsMap;
    private transient ThrottledLogger throttledLogger;

    private final Counter successfulWrites = Metrics.counter(BulkWriteFn.class, "successfulWrites");
    private final Counter retriableFailedWrites =
        Metrics.counter(BulkWriteFn.class, "retriableFailedWrites");
    private final Counter severeFailedWrites =
        Metrics.counter(BulkWriteFn.class, "severeFailedWrites");

    public BulkWriteFn(
        String connectionString,
        String database,
        int batchSize,
        int maxConcurrentAsyncWrites,
        int initialWriteRatePerWorker,
        int writeRateRampUpMinutes,
        int writeRateRampUpSteps,
        int maxWriteRatePerWorker,
        int maxWriteRetries,
        int dlqMaxRetries,
        SerializableFunction<String, MongoClient> clientFactory,
        TupleTag<MongoDbChangeEventContext> successTag,
        TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>> failureTag,
        TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
            severeFailureTag) {
      this.connectionString = connectionString;
      this.database = database;
      this.batchSize = batchSize > 0 ? batchSize : 500;
      this.maxConcurrentAsyncWrites = maxConcurrentAsyncWrites > 0 ? maxConcurrentAsyncWrites : 10;
      this.initialWriteRatePerWorker = initialWriteRatePerWorker;
      this.writeRateRampUpMinutes = writeRateRampUpMinutes > 0 ? writeRateRampUpMinutes : 5;
      this.writeRateRampUpSteps = writeRateRampUpSteps > 0 ? writeRateRampUpSteps : 5;
      this.maxWriteRatePerWorker = maxWriteRatePerWorker > 0 ? maxWriteRatePerWorker : 2500;
      this.maxWriteRetries = maxWriteRetries >= 0 ? maxWriteRetries : 3;
      this.dlqMaxRetries = dlqMaxRetries >= 0 ? dlqMaxRetries : 3;
      this.clientFactory = clientFactory;
      this.successTag = successTag;
      this.failureTag = failureTag;
      this.severeFailureTag = severeFailureTag;
    }

    public static Builder builder() {
      return new Builder();
    }

    public static class Builder {
      private String connectionString;
      private String database;
      private int batchSize = 500;
      private int maxConcurrentAsyncWrites = 10;
      private int initialWriteRatePerWorker = 500;
      private int writeRateRampUpMinutes = 5;
      private int writeRateRampUpSteps = 5;
      private int maxWriteRatePerWorker = 2500;
      private int maxWriteRetries = 3;
      private int dlqMaxRetries = 3;
      private SerializableFunction<String, MongoClient> clientFactory =
          MongoDbBulkTransforms::createMongoClient;
      private TupleTag<MongoDbChangeEventContext> successTag = SUCCESSFUL_WRITE_TAG;
      private TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
          failureTag = FAILED_WRITE_TAG;
      private TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
          severeFailureTag = SEVERE_FAILED_WRITE_TAG;

      public Builder withConnectionString(String connectionString) {
        this.connectionString = connectionString;
        return this;
      }

      public Builder withUri(String uri) {
        this.connectionString = uri;
        return this;
      }

      public Builder withDatabase(String database) {
        this.database = database;
        return this;
      }

      public Builder withBatchSize(Integer batchSize) {
        if (batchSize != null) {
          this.batchSize = batchSize;
        }
        return this;
      }

      public Builder withMaxConcurrentAsyncWrites(Integer maxConcurrentAsyncWrites) {
        if (maxConcurrentAsyncWrites != null) {
          this.maxConcurrentAsyncWrites = maxConcurrentAsyncWrites;
        }
        return this;
      }

      public Builder withInitialWriteRatePerWorker(Integer initialWriteRatePerWorker) {
        if (initialWriteRatePerWorker != null) {
          this.initialWriteRatePerWorker = initialWriteRatePerWorker;
        }
        return this;
      }

      public Builder withWriteRateRampUpMinutes(Integer writeRateRampUpMinutes) {
        if (writeRateRampUpMinutes != null) {
          this.writeRateRampUpMinutes = writeRateRampUpMinutes;
        }
        return this;
      }

      public Builder withWriteRateRampUpSteps(Integer writeRateRampUpSteps) {
        if (writeRateRampUpSteps != null) {
          this.writeRateRampUpSteps = writeRateRampUpSteps;
        }
        return this;
      }

      public Builder withMaxWriteRatePerWorker(Integer maxWriteRatePerWorker) {
        if (maxWriteRatePerWorker != null) {
          this.maxWriteRatePerWorker = maxWriteRatePerWorker;
        }
        return this;
      }

      public Builder withMaxWriteRetries(Integer maxWriteRetries) {
        if (maxWriteRetries != null) {
          this.maxWriteRetries = maxWriteRetries;
        }
        return this;
      }

      public Builder withDlqMaxRetries(Integer dlqMaxRetries) {
        if (dlqMaxRetries != null) {
          this.dlqMaxRetries = dlqMaxRetries;
        }
        return this;
      }

      public Builder withClientFactory(SerializableFunction<String, MongoClient> clientFactory) {
        if (clientFactory != null) {
          this.clientFactory = clientFactory;
        }
        return this;
      }

      public Builder withSuccessTag(TupleTag<MongoDbChangeEventContext> successTag) {
        this.successTag = successTag;
        return this;
      }

      public Builder withFailureTag(
          TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
              failureTag) {
        this.failureTag = failureTag;
        return this;
      }

      public Builder withSevereFailureTag(
          TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
              severeFailureTag) {
        this.severeFailureTag = severeFailureTag;
        return this;
      }

      public BulkWriteFn build() {
        return new BulkWriteFn(
            connectionString,
            database,
            batchSize,
            maxConcurrentAsyncWrites,
            initialWriteRatePerWorker,
            writeRateRampUpMinutes,
            writeRateRampUpSteps,
            maxWriteRatePerWorker,
            maxWriteRetries,
            dlqMaxRetries,
            clientFactory,
            successTag,
            failureTag,
            severeFailureTag);
      }
    }

    @Setup
    public void setup() {
      if (mongoClient == null) {
        if (clientFactory != null) {
          mongoClient = clientFactory.apply(connectionString);
        } else {
          mongoClient = createMongoClient(connectionString);
        }
      }

      int threads = Math.max(1, maxConcurrentAsyncWrites);
      this.executorService = Executors.newFixedThreadPool(threads);
      this.semaphore = new Semaphore(threads);

      double initialRate =
          (initialWriteRatePerWorker > 0)
              ? initialWriteRatePerWorker
              : Math.max(1.0, maxWriteRatePerWorker);
      if (initialWriteRatePerWorker > 0 || maxWriteRatePerWorker > 0) {
        this.rateLimiter = RateLimiter.create(Math.max(1.0, initialRate));
      } else {
        this.rateLimiter = null;
      }

      this.setupStartTimeMs = System.currentTimeMillis();
      this.currentBatches = new ConcurrentHashMap<>();
      this.inFlightFutures = new ArrayList<>();
      this.successQueue = new ConcurrentLinkedQueue<>();
      this.failureQueue = new ConcurrentLinkedQueue<>();
      this.severeFailureQueue = new ConcurrentLinkedQueue<>();
      this.collectionsMap = new ConcurrentHashMap<>();
      this.throttledLogger = new ThrottledLogger("BulkWriteFn", 30000L);
    }

    @StartBundle
    public void startBundle() {
      if (currentBatches != null) {
        currentBatches.clear();
      }
      if (inFlightFutures != null) {
        inFlightFutures.clear();
      }
      if (successQueue != null) {
        successQueue.clear();
      }
      if (failureQueue != null) {
        failureQueue.clear();
      }
      if (severeFailureQueue != null) {
        severeFailureQueue.clear();
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      MongoDbChangeEventContext event = context.element();
      if (event == null) {
        return;
      }
      String collectionName = event.getDataCollection();
      List<MongoDbChangeEventContext> batch =
          currentBatches.computeIfAbsent(collectionName, k -> new ArrayList<>());
      batch.add(event);

      if (batch.size() >= batchSize) {
        flushBatch(collectionName, batch);
      }

      drainQueues(context);
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) {
      if (currentBatches != null) {
        for (Map.Entry<String, List<MongoDbChangeEventContext>> entry : currentBatches.entrySet()) {
          if (!entry.getValue().isEmpty()) {
            flushBatch(entry.getKey(), entry.getValue());
          }
        }
      }

      try {
        if (inFlightFutures != null && !inFlightFutures.isEmpty()) {
          CompletableFuture.allOf(inFlightFutures.toArray(new CompletableFuture[0])).join();
        }
      } finally {
        drainQueuesFinishBundle(context);
        if (inFlightFutures != null) {
          inFlightFutures.clear();
        }
      }
    }

    @Teardown
    public void teardown() {
      if (executorService != null) {
        executorService.shutdown();
        try {
          if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
          }
        } catch (InterruptedException e) {
          executorService.shutdownNow();
          Thread.currentThread().interrupt();
        }
      }
      if (mongoClient != null) {
        mongoClient.close();
        mongoClient = null;
      }
    }

    private void flushBatch(String collectionName, List<MongoDbChangeEventContext> batch) {
      if (batch == null || batch.isEmpty()) {
        return;
      }
      List<MongoDbChangeEventContext> batchToExecute = new ArrayList<>(batch);
      batch.clear();

      try {
        semaphore.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted acquiring semaphore permit", e);
      }

      CompletableFuture<Void> future =
          CompletableFuture.runAsync(
              () -> {
                try {
                  executeBatch(collectionName, batchToExecute);
                } finally {
                  semaphore.release();
                }
              },
              executorService);

      inFlightFutures.add(future);
    }

    private void applyRateLimiter(int permits) {
      if (rateLimiter == null || permits <= 0) {
        return;
      }
      if (initialWriteRatePerWorker > 0 && maxWriteRatePerWorker > 0) {
        long elapsedMs = System.currentTimeMillis() - setupStartTimeMs;
        long rampUpMs = writeRateRampUpMinutes * 60 * 1000L;
        double currentRate;
        if (rampUpMs <= 0 || elapsedMs >= rampUpMs) {
          currentRate = maxWriteRatePerWorker;
        } else {
          double fraction = (double) elapsedMs / rampUpMs;
          if (writeRateRampUpSteps > 0) {
            fraction = Math.floor(fraction * writeRateRampUpSteps) / writeRateRampUpSteps;
          }
          currentRate =
              initialWriteRatePerWorker
                  + (maxWriteRatePerWorker - initialWriteRatePerWorker) * fraction;
        }
        double targetRate = Math.max(1.0, currentRate);
        if (Math.abs(rateLimiter.getRate() - targetRate) > 1e-3) {
          rateLimiter.setRate(targetRate);
        }
      }
      rateLimiter.acquire(permits);
    }

    private void executeBatch(String collectionName, List<MongoDbChangeEventContext> batch) {
      if (batch == null || batch.isEmpty()) {
        return;
      }

      List<MongoDbChangeEventContext> activeBatch = new ArrayList<>();
      Map<Object, List<MongoDbChangeEventContext>> supersededPerDoc = new LinkedHashMap<>();

      try {
        applyRateLimiter(batch.size());

        // Intra-batch coalescing: coalesce operations per document ID within the batch to ensure
        // only
        // the latest state is written
        Map<Object, MongoDbChangeEventContext> latestPerDoc = new LinkedHashMap<>();
        for (MongoDbChangeEventContext event : batch) {
          if (event == null) {
            continue;
          }
          Object docId = event.getDocumentId();
          MongoDbChangeEventContext existing = latestPerDoc.get(docId);
          if (existing == null) {
            latestPerDoc.put(docId, event);
          } else {
            TimestampSortKey eventKey = TimestampSortKey.of(event);
            TimestampSortKey existingKey = TimestampSortKey.of(existing);
            int cmp =
                (existingKey == null)
                    ? 1
                    : (eventKey == null ? -1 : eventKey.compareTo(existingKey));
            if (cmp > 0 || (cmp == 0 && event.getIsDlqReconsumed())) {
              supersededPerDoc.computeIfAbsent(docId, k -> new ArrayList<>()).add(existing);
              latestPerDoc.put(docId, event);
            } else {
              supersededPerDoc.computeIfAbsent(docId, k -> new ArrayList<>()).add(event);
            }
          }
        }

        MongoCollection<Document> collection = getCollection(collectionName);
        List<WriteModel<Document>> operations = new ArrayList<>(latestPerDoc.size());

        for (MongoDbChangeEventContext event : latestPerDoc.values()) {
          Object docId = event.getDocumentId();
          Bson lookupById = eq("_id", docId);
          if (event.isDeleteEvent()) {
            operations.add(new DeleteOneModel<>(lookupById));
            activeBatch.add(event);
          } else {
            Document doc = Utils.jsonToDocument(event.getDataAsJsonString(), docId);
            if (doc == null) {
              if (event.isUpdateEvent()) {
                // Null data on update event occurs when doc was deleted right after update; skip
                LOG.info(
                    "Skipping update event for document ID: {} because 'data' field is null",
                    docId);
                successQueue.add(event);
                successfulWrites.inc();
                List<MongoDbChangeEventContext> superseded = supersededPerDoc.remove(docId);
                if (superseded != null && !superseded.isEmpty()) {
                  successQueue.addAll(superseded);
                  successfulWrites.inc(superseded.size());
                }
              } else {
                // Missing document data for non-delete/non-update event -> send to severe DLQ
                FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>
                    severeElement = FailsafeElement.of(event, event);
                severeElement.setErrorMessage("Missing or null document data for docId: " + docId);
                severeFailureQueue.add(severeElement);
                severeFailedWrites.inc();
                supersededPerDoc.remove(docId);
              }
            } else {
              operations.add(
                  new ReplaceOneModel<>(lookupById, doc, new ReplaceOptions().upsert(true)));
              activeBatch.add(event);
            }
          }
        }

        if (operations.isEmpty()) {
          return;
        }

        BulkWriteResult result =
            collection.bulkWrite(operations, new BulkWriteOptions().ordered(false));
        for (MongoDbChangeEventContext event : activeBatch) {
          successQueue.add(event);
          successfulWrites.inc();
          List<MongoDbChangeEventContext> superseded =
              supersededPerDoc.remove(event.getDocumentId());
          if (superseded != null && !superseded.isEmpty()) {
            successQueue.addAll(superseded);
            successfulWrites.inc(superseded.size());
          }
        }
      } catch (MongoBulkWriteException mbwe) {
        handleBulkWriteException(collectionName, activeBatch, supersededPerDoc, mbwe);
      } catch (Exception e) {
        List<MongoDbChangeEventContext> eventsToHandle =
            (!activeBatch.isEmpty()) ? activeBatch : batch;
        handleGeneralBatchException(collectionName, eventsToHandle, supersededPerDoc, e);
      }
    }

    private void handleBulkWriteException(
        String collectionName,
        List<MongoDbChangeEventContext> batch,
        Map<Object, List<MongoDbChangeEventContext>> supersededPerDoc,
        MongoBulkWriteException mbwe) {
      Set<Integer> failedIndices = new HashSet<>();
      List<MongoDbChangeEventContext> transientEvents = new ArrayList<>();
      WriteConcernError writeConcernError = mbwe.getWriteConcernError();

      for (BulkWriteError error : mbwe.getWriteErrors()) {
        int idx = error.getIndex();
        failedIndices.add(idx);
        if (idx >= 0 && idx < batch.size()) {
          MongoDbChangeEventContext event = batch.get(idx);
          int code = error.getCode();
          if (isPermanentErrorCode(code)) {
            FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> severeElement =
                FailsafeElement.of(event, event);
            severeElement.setErrorMessage(
                "Permanent write error (Code " + code + "): " + error.getMessage());
            severeElement.setStacktrace(Throwables.getStackTraceAsString(mbwe));
            severeFailureQueue.add(severeElement);
            severeFailedWrites.inc();
            if (supersededPerDoc != null) {
              supersededPerDoc.remove(event.getDocumentId());
            }
          } else {
            transientEvents.add(event);
          }
        }
      }

      // If writeConcernError occurred, retry all non-permanently failed events in batch
      if (writeConcernError != null) {
        throttledLogger.logWarn(
            LOG,
            collectionName,
            "Encountered WriteConcernError: {}, retrying unconfirmed batch",
            writeConcernError.getMessage());
        for (int i = 0; i < batch.size(); i++) {
          if (!failedIndices.contains(i)) {
            transientEvents.add(batch.get(i));
          }
        }
      } else {
        // Output successful documents only when no write concern error occurred
        for (int i = 0; i < batch.size(); i++) {
          if (!failedIndices.contains(i)) {
            MongoDbChangeEventContext successfulEvent = batch.get(i);
            successQueue.add(successfulEvent);
            successfulWrites.inc();
            if (supersededPerDoc != null) {
              List<MongoDbChangeEventContext> superseded =
                  supersededPerDoc.remove(successfulEvent.getDocumentId());
              if (superseded != null && !superseded.isEmpty()) {
                successQueue.addAll(superseded);
                successfulWrites.inc(superseded.size());
              }
            }
          }
        }
      }

      // Retry transient documents with backoff
      if (!transientEvents.isEmpty()) {
        retryTransientEvents(collectionName, transientEvents, supersededPerDoc);
      }
    }

    private void handleGeneralBatchException(
        String collectionName,
        List<MongoDbChangeEventContext> batch,
        Map<Object, List<MongoDbChangeEventContext>> supersededPerDoc,
        Exception e) {
      int code = 0;
      if (e instanceof MongoException me) {
        code = me.getCode();
      }

      if (isPermanentErrorCode(code)) {
        throttledLogger.logError(
            LOG,
            collectionName,
            "Permanent failure ({}) during bulkWrite: {}",
            code,
            e.getMessage());
        for (MongoDbChangeEventContext event : batch) {
          FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> severeElement =
              FailsafeElement.of(event, event);
          severeElement.setErrorMessage("Permanent failure: " + e.getMessage());
          severeElement.setStacktrace(Throwables.getStackTraceAsString(e));
          severeFailureQueue.add(severeElement);
          severeFailedWrites.inc();
          if (supersededPerDoc != null) {
            supersededPerDoc.remove(event.getDocumentId());
          }
        }
        return;
      }

      throttledLogger.logWarn(
          LOG,
          collectionName,
          "Batch write encountered retryable error for collection {}: {}",
          collectionName,
          e.getMessage());
      retryTransientEvents(collectionName, batch, supersededPerDoc);
    }

    private void retryTransientEvents(
        String collectionName,
        List<MongoDbChangeEventContext> events,
        Map<Object, List<MongoDbChangeEventContext>> supersededPerDoc) {
      FluentBackoff backoff =
          FluentBackoff.DEFAULT
              .withInitialBackoff(Duration.standardSeconds(2))
              .withExponent(2.0)
              .withMaxRetries(maxWriteRetries);
      BackOff backoffInstance = backoff.backoff();
      Sleeper sleeper = Sleeper.DEFAULT;

      List<MongoDbChangeEventContext> currentRemaining = new ArrayList<>(events);
      while (!currentRemaining.isEmpty()) {
        List<MongoDbChangeEventContext> activeRetryBatch = new ArrayList<>(currentRemaining.size());
        try {
          MongoCollection<Document> collection = getCollection(collectionName);
          List<WriteModel<Document>> operations = new ArrayList<>(currentRemaining.size());
          for (MongoDbChangeEventContext event : currentRemaining) {
            Object docId = event.getDocumentId();
            Bson lookupById = eq("_id", docId);
            if (event.isDeleteEvent()) {
              operations.add(new DeleteOneModel<>(lookupById));
              activeRetryBatch.add(event);
            } else {
              Document doc = Utils.jsonToDocument(event.getDataAsJsonString(), docId);
              if (doc == null) {
                if (event.isUpdateEvent()) {
                  successQueue.add(event);
                  successfulWrites.inc();
                  if (supersededPerDoc != null) {
                    List<MongoDbChangeEventContext> superseded = supersededPerDoc.remove(docId);
                    if (superseded != null && !superseded.isEmpty()) {
                      successQueue.addAll(superseded);
                      successfulWrites.inc(superseded.size());
                    }
                  }
                } else {
                  FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>
                      severeElement = FailsafeElement.of(event, event);
                  severeElement.setErrorMessage(
                      "Missing or null document data on retry for docId: " + docId);
                  severeFailureQueue.add(severeElement);
                  severeFailedWrites.inc();
                  if (supersededPerDoc != null) {
                    supersededPerDoc.remove(docId);
                  }
                }
              } else {
                operations.add(
                    new ReplaceOneModel<>(lookupById, doc, new ReplaceOptions().upsert(true)));
                activeRetryBatch.add(event);
              }
            }
          }

          if (operations.isEmpty()) {
            currentRemaining.clear();
            break;
          }

          collection.bulkWrite(operations, new BulkWriteOptions().ordered(false));
          for (MongoDbChangeEventContext event : activeRetryBatch) {
            successQueue.add(event);
            successfulWrites.inc();
            if (supersededPerDoc != null) {
              List<MongoDbChangeEventContext> superseded =
                  supersededPerDoc.remove(event.getDocumentId());
              if (superseded != null && !superseded.isEmpty()) {
                successQueue.addAll(superseded);
                successfulWrites.inc(superseded.size());
              }
            }
          }
          currentRemaining.clear();
          break;
        } catch (MongoBulkWriteException mbwe) {
          Set<Integer> failedIndices = new HashSet<>();
          List<MongoDbChangeEventContext> nextRetry = new ArrayList<>();
          for (BulkWriteError err : mbwe.getWriteErrors()) {
            int idx = err.getIndex();
            failedIndices.add(idx);
            if (idx >= 0 && idx < activeRetryBatch.size()) {
              MongoDbChangeEventContext ev = activeRetryBatch.get(idx);
              if (isPermanentErrorCode(err.getCode())) {
                FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>
                    severeElement = FailsafeElement.of(ev, ev);
                severeElement.setErrorMessage(
                    "Permanent write error on retry (Code "
                        + err.getCode()
                        + "): "
                        + err.getMessage());
                severeElement.setStacktrace(Throwables.getStackTraceAsString(mbwe));
                severeFailureQueue.add(severeElement);
                severeFailedWrites.inc();
                if (supersededPerDoc != null) {
                  supersededPerDoc.remove(ev.getDocumentId());
                }
              } else {
                nextRetry.add(ev);
              }
            }
          }
          for (int i = 0; i < activeRetryBatch.size(); i++) {
            if (!failedIndices.contains(i)) {
              MongoDbChangeEventContext successfulEvent = activeRetryBatch.get(i);
              successQueue.add(successfulEvent);
              successfulWrites.inc();
              if (supersededPerDoc != null) {
                List<MongoDbChangeEventContext> superseded =
                    supersededPerDoc.remove(successfulEvent.getDocumentId());
                if (superseded != null && !superseded.isEmpty()) {
                  successQueue.addAll(superseded);
                  successfulWrites.inc(superseded.size());
                }
              }
            }
          }
          currentRemaining = nextRetry;
        } catch (Exception e) {
          throttledLogger.logWarn(
              LOG,
              collectionName,
              "Error during retry for collection {}: {}",
              collectionName,
              e.getMessage());
        }

        if (!currentRemaining.isEmpty()) {
          try {
            if (!BackOffUtils.next(sleeper, backoffInstance)) {
              // Backoff exhausted -> route to DLQ
              for (MongoDbChangeEventContext ev : currentRemaining) {
                FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>
                    failedElement = FailsafeElement.of(ev, ev);
                failedElement.setErrorMessage(
                    "Transient write error retries exhausted after "
                        + maxWriteRetries
                        + " attempts");
                failureQueue.add(failedElement);
                retriableFailedWrites.inc();
                if (supersededPerDoc != null) {
                  supersededPerDoc.remove(ev.getDocumentId());
                }
              }
              break;
            }
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            for (MongoDbChangeEventContext ev : currentRemaining) {
              FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> failedElement =
                  FailsafeElement.of(ev, ev);
              failedElement.setErrorMessage("Retry interrupted: " + ie.getMessage());
              failureQueue.add(failedElement);
              retriableFailedWrites.inc();
              if (supersededPerDoc != null) {
                supersededPerDoc.remove(ev.getDocumentId());
              }
            }
            break;
          }
        }
      }
    }

    private MongoCollection<Document> getCollection(String collectionName) {
      return collectionsMap.computeIfAbsent(
          collectionName, k -> mongoClient.getDatabase(database).getCollection(collectionName));
    }

    private void drainQueues(ProcessContext context) {
      drainToOutput(
          (tag, value) -> context.output((TupleTag<MongoDbChangeEventContext>) tag, value),
          (tag, value) ->
              context.output(
                  (TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>)
                      tag,
                  value));
    }

    private void drainQueuesFinishBundle(FinishBundleContext context) {
      drainToOutput(
          (tag, value) ->
              context.output(
                  (TupleTag<MongoDbChangeEventContext>) tag,
                  value,
                  BoundedWindow.TIMESTAMP_MIN_VALUE,
                  GlobalWindow.INSTANCE),
          (tag, value) ->
              context.output(
                  (TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>)
                      tag,
                  value,
                  BoundedWindow.TIMESTAMP_MIN_VALUE,
                  GlobalWindow.INSTANCE));
    }

    private void drainToOutput(
        java.util.function.BiConsumer<TupleTag<?>, MongoDbChangeEventContext> successConsumer,
        java.util.function.BiConsumer<
                TupleTag<?>, FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
            failureConsumer) {
      MongoDbChangeEventContext success;
      while ((success = successQueue.poll()) != null) {
        successConsumer.accept(successTag, success);
      }

      FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> failure;
      while ((failure = failureQueue.poll()) != null) {
        failureConsumer.accept(failureTag, failure);
      }

      FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> severe;
      while ((severe = severeFailureQueue.poll()) != null) {
        failureConsumer.accept(severeFailureTag, severe);
      }
    }

    @VisibleForTesting
    public void setMongoClient(MongoClient client) {
      this.mongoClient = client;
    }
  }

  // Backward compatibility alias
  public static class FirestoreAsyncBulkWriterFn extends BulkWriteFn {
    public FirestoreAsyncBulkWriterFn(
        String connectionString,
        String database,
        int batchSize,
        int maxConcurrentAsyncWrites,
        int initialWriteRatePerWorker,
        int writeRateRampUpMinutes,
        int maxWriteRatePerWorker,
        int maxWriteRetries,
        SerializableFunction<String, MongoClient> clientFactory) {
      super(
          connectionString,
          database,
          batchSize,
          maxConcurrentAsyncWrites,
          initialWriteRatePerWorker,
          writeRateRampUpMinutes,
          5,
          maxWriteRatePerWorker,
          maxWriteRetries,
          3,
          clientFactory,
          SUCCESSFUL_WRITE_TAG,
          FAILED_WRITE_TAG,
          SEVERE_FAILED_WRITE_TAG);
    }

    @VisibleForTesting
    public FirestoreAsyncBulkWriterFn(
        MongoClient mongoClient,
        String databaseName,
        int batchSize,
        int maxConcurrentAsyncWrites,
        int initialWriteRatePerWorker,
        int writeRateRampUpMinutes,
        int maxWriteRatePerWorker,
        int maxWriteRetries) {
      super(
          "mongodb://localhost:27017",
          databaseName,
          batchSize,
          maxConcurrentAsyncWrites,
          initialWriteRatePerWorker,
          writeRateRampUpMinutes,
          5,
          maxWriteRatePerWorker,
          maxWriteRetries,
          3,
          new StubMongoClientFactory(mongoClient),
          SUCCESSFUL_WRITE_TAG,
          FAILED_WRITE_TAG,
          SEVERE_FAILED_WRITE_TAG);
      setMongoClient(mongoClient);
    }
  }

  private static class StubMongoClientFactory
      implements SerializableFunction<String, MongoClient>, Serializable {
    private final transient MongoClient client;

    StubMongoClientFactory(MongoClient client) {
      this.client = client;
    }

    @Override
    public MongoClient apply(String input) {
      return client;
    }
  }
}
