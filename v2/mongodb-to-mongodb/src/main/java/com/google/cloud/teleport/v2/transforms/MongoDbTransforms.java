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

import com.google.cloud.teleport.v2.transforms.DocumentWithMetadata.ErrorType;
import com.google.cloud.teleport.v2.transforms.DocumentWithMetadata.FailureStage;
import com.google.cloud.teleport.v2.transforms.DocumentWithMetadata.OperationType;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptRuntime;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import com.mongodb.ConnectionString;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transforms for the MongoDB to MongoDB template. */
public class MongoDbTransforms {

  private static final ConcurrentHashMap<String, MongoClient> CLIENT_CACHE =
      new ConcurrentHashMap<>();

  /**
   * Returns a cached JVM-wide MongoClient for the given URI, or creates a new one if absent.
   */
  public static MongoClient getOrCreateMongoClient(String uri) {
    return CLIENT_CACHE.computeIfAbsent(uri, MongoDbTransforms::createMongoClient);
  }

  /**
   * Helper method to create a MongoClient with default UuidRepresentation.STANDARD if not
   * explicitly specified in the connection string.
   */
  public static MongoClient createMongoClient(String uri) {
    ConnectionString connectionString = new ConnectionString(uri);
    MongoClientSettings.Builder builder =
        MongoClientSettings.builder().applyConnectionString(connectionString);
    if (connectionString.getUuidRepresentation() == null
        || connectionString.getUuidRepresentation() == UuidRepresentation.UNSPECIFIED) {
      builder.uuidRepresentation(UuidRepresentation.STANDARD);
    }
    if (connectionString.getReadPreference() == null) {
      builder.readPreference(ReadPreference.secondaryPreferred());
    }
    builder.applyToConnectionPoolSettings(
        pool -> pool.maxSize(256).minSize(64).maxWaitTime(30, TimeUnit.SECONDS));
    builder.applyToSocketSettings(
        socket -> socket.connectTimeout(15, TimeUnit.SECONDS).readTimeout(60, TimeUnit.SECONDS));
    return MongoClients.create(builder.build());
  }

  public static WriteWithDlq writeWithDlq() {
    return new WriteWithDlq();
  }

  public static class WriteWithDlq
      extends PTransform<PCollection<DocumentWithMetadata>, PCollection<DocumentWithMetadata>> {
    private String uri;
    private String database;
    private Integer batchSize = 5000;

    private Integer maxConcurrentAsyncWrites = 10;
    private Integer maxWriteRetries = 3;
    private Integer dlqMaxRetries = 3;
    private Integer initialWriteRatePerWorker = 5000;
    private Integer writeRateRampUpMinutes = 5;
    private Integer writeRateRampUpSteps = 5;
    private Integer maxWriteRatePerWorker = 25000;
    private SerializableFunction<String, MongoClient> clientFactory =
        MongoDbTransforms::createMongoClient;

    private Integer numWriteShards = 64;
    private Integer maxBufferingDurationMs = 200;

    public WriteWithDlq withUri(String uri) {
      this.uri = uri;
      return this;
    }

    public WriteWithDlq withDatabase(String database) {
      this.database = database;
      return this;
    }

    public WriteWithDlq withBatchSize(Integer batchSize) {
      if (batchSize != null) {
        this.batchSize = batchSize;
      }
      return this;
    }

    public WriteWithDlq withNumWriteShards(Integer numWriteShards) {
      if (numWriteShards != null) {
        this.numWriteShards = numWriteShards;
      }
      return this;
    }

    public WriteWithDlq withMaxBufferingDurationMs(Integer maxBufferingDurationMs) {
      if (maxBufferingDurationMs != null) {
        this.maxBufferingDurationMs = maxBufferingDurationMs;
      }
      return this;
    }

    public WriteWithDlq withMaxConcurrentAsyncWrites(Integer maxConcurrentAsyncWrites) {
      if (maxConcurrentAsyncWrites != null) {
        this.maxConcurrentAsyncWrites = maxConcurrentAsyncWrites;
      }
      return this;
    }

    public WriteWithDlq withMaxWriteRetries(Integer maxWriteRetries) {
      if (maxWriteRetries != null) {
        this.maxWriteRetries = maxWriteRetries;
      }
      return this;
    }

    public WriteWithDlq withDlqMaxRetries(Integer dlqMaxRetries) {
      if (dlqMaxRetries != null) {
        this.dlqMaxRetries = dlqMaxRetries;
      }
      return this;
    }

    public WriteWithDlq withInitialWriteRatePerWorker(Integer initialWriteRatePerWorker) {
      if (initialWriteRatePerWorker != null) {
        this.initialWriteRatePerWorker = initialWriteRatePerWorker;
      }
      return this;
    }

    public WriteWithDlq withWriteRateRampUpMinutes(Integer writeRateRampUpMinutes) {
      if (writeRateRampUpMinutes != null) {
        this.writeRateRampUpMinutes = writeRateRampUpMinutes;
      }
      return this;
    }

    public WriteWithDlq withWriteRateRampUpSteps(Integer writeRateRampUpSteps) {
      if (writeRateRampUpSteps != null) {
        this.writeRateRampUpSteps = writeRateRampUpSteps;
      }
      return this;
    }

    public WriteWithDlq withMaxWriteRatePerWorker(Integer maxWriteRatePerWorker) {
      if (maxWriteRatePerWorker != null) {
        this.maxWriteRatePerWorker = maxWriteRatePerWorker;
      }
      return this;
    }

    public WriteWithDlq withClientFactory(SerializableFunction<String, MongoClient> clientFactory) {
      if (clientFactory != null) {
        this.clientFactory = clientFactory;
      }
      return this;
    }

    @Override
    public PCollection<DocumentWithMetadata> expand(PCollection<DocumentWithMetadata> input) {
      TupleTag<DocumentWithMetadata> successTag = new TupleTag<DocumentWithMetadata>() {};
      TupleTag<DocumentWithMetadata> failureTag = new TupleTag<DocumentWithMetadata>() {};

      int shards = (numWriteShards != null && numWriteShards > 0) ? numWriteShards : 64;
      int size = (batchSize != null && batchSize > 0) ? batchSize : 1000;

      GroupIntoBatches<String, DocumentWithMetadata> groupTransform =
          GroupIntoBatches.<String, DocumentWithMetadata>ofSize(size);
      if (maxBufferingDurationMs != null && maxBufferingDurationMs > 0) {
        groupTransform =
            groupTransform.withMaxBufferingDuration(Duration.millis(maxBufferingDurationMs));
      }

      PCollectionTuple writeResults =
          input
              .apply(
                  "KeyByWriteShard",
                  ParDo.of(
                      new DoFn<DocumentWithMetadata, KV<String, DocumentWithMetadata>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          DocumentWithMetadata item = c.element();
                          if (item == null) {
                            return;
                          }
                          String col = item.getTargetCollection();
                          if (col == null) {
                            col = item.getSourceCollection();
                          }
                          String dedupKey = item.getDedupKey();
                          int shard =
                              dedupKey != null
                                  ? Math.floorMod(dedupKey.hashCode(), shards)
                                  : 0;
                          c.output(KV.of(col + "#" + shard, item));
                        }
                      }))
              .setCoder(KvCoder.of(StringUtf8Coder.of(), DocumentWithMetadataCoder.of()))
              .apply("GroupIntoBatches", groupTransform)
              .apply(
                  "WriteBatches",
                  ParDo.of(
                          WriteBatchesFn.builder()
                              .withUri(uri)
                              .withDatabase(database)
                              .withMaxConcurrentAsyncWrites(maxConcurrentAsyncWrites)
                              .withMaxWriteRetries(maxWriteRetries)
                              .withDlqMaxRetries(dlqMaxRetries)
                              .withInitialWriteRatePerWorker(initialWriteRatePerWorker)
                              .withWriteRateRampUpMinutes(writeRateRampUpMinutes)
                              .withWriteRateRampUpSteps(writeRateRampUpSteps)
                              .withMaxWriteRatePerWorker(maxWriteRatePerWorker)
                              .withClientFactory(clientFactory)
                              .withFailureTag(failureTag)
                              .build())
                      .withOutputTags(successTag, TupleTagList.of(failureTag)));

      return writeResults.get(failureTag);
    }
  }

  public static class WriteToDlq extends PTransform<PCollection<DocumentWithMetadata>, PDone> {
    private static final Logger LOG = LoggerFactory.getLogger(WriteToDlq.class);
    private final String retryablePath;
    private final String permanentPath;
    @SuppressWarnings("unused")
    private final String tempLocation;

    private final Counter dlqRetryableEmitted =
        Metrics.counter(WriteToDlq.class, "dlqRetryableEmitted");
    private final Counter dlqPermanentEmitted =
        Metrics.counter(WriteToDlq.class, "dlqPermanentEmitted");

    public WriteToDlq(String retryablePath, String permanentPath, String tempLocation) {
      this.retryablePath = retryablePath;
      this.permanentPath = permanentPath;
      this.tempLocation = tempLocation;
    }

    @Override
    public PDone expand(PCollection<DocumentWithMetadata> input) {
      LOG.info("Configuring DLQ Retryable Output Path: {}", retryablePath);
      LOG.info("Configuring DLQ Permanent Output Path: {}", permanentPath);
      LOG.info(
          "To inspect permanent DLQ errors, run: gcloud storage cat \"{}/**/output-*\" | head -n 5",
          permanentPath);

      PCollection<DocumentWithMetadata> retryable =
          input.apply("FilterRetryable", Filter.by(item -> item.getErrorType() == RETRYABLE));

      PCollection<DocumentWithMetadata> permanent =
          input.apply("FilterPermanent", Filter.by(item -> item.getErrorType() == PERMANENT));

      boolean isUnbounded = input.isBounded() == PCollection.IsBounded.UNBOUNDED;

      PCollection<String> retryableJson =
          retryable.apply(
              "MapToJson_Retryable",
              ParDo.of(
                  new DoFn<DocumentWithMetadata, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      dlqRetryableEmitted.inc();
                      DocumentWithMetadata item = c.element();
                      c.output(
                          item.toDlqJson(
                              item.getErrorMessage(), item.getErrorType(), item.getRetryCount()));
                    }
                  }));

      if (isUnbounded) {
        retryableJson
            .apply("WindowRetryableDlq", Window.into(FixedWindows.of(Duration.standardMinutes(1))))
            .apply(
                "WriteDlq_Retryable",
                TextIO.write()
                    .to(retryablePath + "/error")
                    .withSuffix(".json")
                    .withWindowedWrites()
                    .withNumShards(1));
      } else {
        retryableJson.apply(
            "WriteDlq_Retryable",
            TextIO.write().to(retryablePath + "/error").withSuffix(".json"));
      }

      PCollection<String> permanentJson =
          permanent.apply(
              "MapToJson_Permanent",
              ParDo.of(
                  new DoFn<DocumentWithMetadata, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      dlqPermanentEmitted.inc();
                      DocumentWithMetadata item = c.element();
                      c.output(
                          item.toDlqJson(
                              item.getErrorMessage(), item.getErrorType(), item.getRetryCount()));
                    }
                  }));

      if (isUnbounded) {
        permanentJson
            .apply("WindowPermanentDlq", Window.into(FixedWindows.of(Duration.standardMinutes(1))))
            .apply(
                "WriteDlq_Permanent",
                TextIO.write()
                    .to(permanentPath + "/error")
                    .withSuffix(".json")
                    .withWindowedWrites()
                    .withNumShards(1));
      } else {
        permanentJson.apply(
            "WriteDlq_Permanent",
            TextIO.write().to(permanentPath + "/error").withSuffix(".json"));
      }

      return PDone.in(input.getPipeline());
    }
  }

  /** A {@link DoFn} that writes documents to MongoDB in bulk. */
  public static class WriteFn extends DoFn<DocumentWithMetadata, DocumentWithMetadata> {

    private static final int ERR_DOCUMENT_VALIDATION_FAILURE = 121;
    private static final int ERR_KEY_TOO_LONG = 17280;
    private static final int ERR_BAD_VALUE = 2;
    private static final long DLQ_LOG_INTERVAL_MS = 30_000L;

    private static final Logger LOG = LoggerFactory.getLogger(WriteFn.class);

    private final String uri;
    private final String database;
    private final Integer batchSize;
    private final Integer maxConcurrentAsyncWrites;
    private final Integer maxWriteRetries;
    private final Integer dlqMaxRetries;
    private final Integer initialWriteRatePerWorker;
    private final Integer writeRateRampUpMinutes;
    private final Integer writeRateRampUpSteps;
    private final Integer maxWriteRatePerWorker;
    private final SerializableFunction<String, MongoClient> clientFactory;
    private final TupleTag<DocumentWithMetadata> failureTag;
    private transient FluentBackoff backoffSpec;
    private transient RateLimiter rateLimiter;
    private transient long startTimeMs;
    private transient long lastComputedStep;

    private final Counter successfulWrites =
        Metrics.counter(WriteWithDlq.class, "successfulWrites");
    private final Counter inMemoryRetries = Metrics.counter(WriteWithDlq.class, "inMemoryRetries");
    private final Counter severeFailedWrites =
        Metrics.counter(WriteWithDlq.class, "severeFailedWrites");
    private final Counter dlqRetries = Metrics.counter(WriteWithDlq.class, "dlqRetries");
    private final Counter permanentFailures =
        Metrics.counter(WriteWithDlq.class, "permanentFailures");
    private final Counter batchesFlushed =
        Metrics.counter(WriteWithDlq.class, "batchesFlushed");
    private final Counter writeInsertsUpserts =
        Metrics.counter(WriteWithDlq.class, "writeInsertsUpserts");
    private final Counter writeDeletes =
        Metrics.counter(WriteWithDlq.class, "writeDeletes");
    private final Counter writeDropsSkipped =
        Metrics.counter(WriteWithDlq.class, "writeDropsSkipped");

    private transient MongoClient mongoClient;
    private transient ExecutorService executor;
    private transient Semaphore semaphore;
    private transient ConcurrentLinkedQueue<CompletableFuture<Void>> futures;
    private transient ConcurrentLinkedQueue<DocumentWithMetadata> failures;
    private transient AtomicLong successfulCount;
    private transient ConcurrentHashMap<String, AtomicLong> dynamicCounters;
    private transient AtomicLong inMemoryRetriesCount;
    private transient AtomicLong severeFailedWritesCount;
    private transient AtomicLong dlqRetriesCount;
    private transient AtomicLong permanentFailuresCount;
    private transient List<DocumentWithMetadata> currentBatch;
    private transient long lastDlqLogTimeMs;

    private void incDynamicCounter(String prefix, String exceptionName, int code, long count) {
      String counterName = prefix + "_" + exceptionName + "_" + code;
      if (dynamicCounters != null) {
        dynamicCounters.computeIfAbsent(counterName, k -> new AtomicLong(0)).addAndGet(count);
      }
    }

    public WriteFn(
        String uri,
        String database,
        Integer batchSize,
        Integer maxConcurrentAsyncWrites,
        Integer maxWriteRetries,
        Integer dlqMaxRetries,
        Integer initialWriteRatePerWorker,
        Integer writeRateRampUpMinutes,
        Integer writeRateRampUpSteps,
        Integer maxWriteRatePerWorker,
        SerializableFunction<String, MongoClient> clientFactory,
        TupleTag<DocumentWithMetadata> failureTag) {
      this.uri = uri;
      this.database = database;
      this.batchSize = batchSize;
      this.maxConcurrentAsyncWrites = maxConcurrentAsyncWrites;
      this.maxWriteRetries = maxWriteRetries;
      this.dlqMaxRetries = dlqMaxRetries;
      this.initialWriteRatePerWorker = initialWriteRatePerWorker;
      this.writeRateRampUpMinutes = writeRateRampUpMinutes;
      this.writeRateRampUpSteps = writeRateRampUpSteps;
      this.maxWriteRatePerWorker = maxWriteRatePerWorker;
      this.clientFactory = clientFactory;
      this.failureTag = failureTag;
    }

    public static Builder builder() {
      return new Builder();
    }

    public static class Builder {
      private String uri;
      private String database;
      private Integer batchSize = 5000;
      private Integer maxConcurrentAsyncWrites = 10;
      private Integer maxWriteRetries = 3;
      private Integer dlqMaxRetries = 3;
      private Integer initialWriteRatePerWorker = 5000;
      private Integer writeRateRampUpMinutes = 5;
      private Integer writeRateRampUpSteps = 5;
      private Integer maxWriteRatePerWorker = 25000;
      private SerializableFunction<String, MongoClient> clientFactory;
      private TupleTag<DocumentWithMetadata> failureTag;

      public Builder withUri(String uri) {
        this.uri = uri;
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

      public Builder withMaxWriteRetries(Integer maxWriteRetries) {
        if (maxWriteRetries != null) {
          this.maxWriteRetries = maxWriteRetries;
        }
        return this;
      }

      public Builder withDlqMaxRetries(Integer dlqMaxRetries) {
        this.dlqMaxRetries = dlqMaxRetries;
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

      public Builder withClientFactory(SerializableFunction<String, MongoClient> clientFactory) {
        this.clientFactory = clientFactory;
        return this;
      }

      public Builder withFailureTag(TupleTag<DocumentWithMetadata> failureTag) {
        this.failureTag = failureTag;
        return this;
      }

      public WriteFn build() {
        return new WriteFn(
            uri,
            database,
            batchSize,
            maxConcurrentAsyncWrites,
            maxWriteRetries,
            dlqMaxRetries,
            initialWriteRatePerWorker,
            writeRateRampUpMinutes,
            writeRateRampUpSteps,
            maxWriteRatePerWorker,
            clientFactory,
            failureTag);
      }
    }

    @VisibleForTesting
    RateLimiter getRateLimiter() {
      return rateLimiter;
    }

    @VisibleForTesting
    void setStartTimeMs(long startTimeMs) {
      this.startTimeMs = startTimeMs;
    }

    @VisibleForTesting
    void updateRateLimiterForTest() {
      updateRateLimiterIfNeeded();
    }

    @Setup
    public void setup() {
      executor = Executors.newFixedThreadPool(maxConcurrentAsyncWrites);
      semaphore = new Semaphore(maxConcurrentAsyncWrites);
      if (clientFactory != null && uri != null) {
        mongoClient = clientFactory.apply(uri);
      }
      backoffSpec =
          FluentBackoff.DEFAULT
              .withMaxRetries(maxWriteRetries)
              .withInitialBackoff(Duration.standardSeconds(2))
              .withExponent(2.0);
      if (initialWriteRatePerWorker != null && initialWriteRatePerWorker > 0) {
        rateLimiter = RateLimiter.create(initialWriteRatePerWorker);
        startTimeMs = System.currentTimeMillis();
        lastComputedStep = 0;
        LOG.info(
            "Enabled linear write rate ramp-up: initialRate={} docs/s/worker, targetMax={}"
                + " docs/s/worker, duration={} mins, steps={}",
            initialWriteRatePerWorker,
            maxWriteRatePerWorker,
            writeRateRampUpMinutes,
            writeRateRampUpSteps);
      } else {
        rateLimiter = null;
        LOG.info("Write rate limiting is disabled (initialWriteRatePerWorker <= 0)");
      }
      LOG.info(
          "Initialized MongoDB WriteFn worker thread for database '{}' (batchSize={},"
              + " maxConcurrentAsyncWrites={}, maxWriteRetries={})",
          database,
          batchSize,
          maxConcurrentAsyncWrites,
          maxWriteRetries);
    }

    private void updateRateLimiterIfNeeded() {
      if (rateLimiter == null
          || writeRateRampUpMinutes == null
          || writeRateRampUpMinutes <= 0
          || writeRateRampUpSteps == null
          || writeRateRampUpSteps <= 0
          || maxWriteRatePerWorker == null
          || maxWriteRatePerWorker <= initialWriteRatePerWorker) {
        return;
      }
      long stepDurationMs = (writeRateRampUpMinutes * 60L * 1000L) / writeRateRampUpSteps;
      if (stepDurationMs <= 0) {
        stepDurationMs = 1;
      }
      long elapsedMs = System.currentTimeMillis() - startTimeMs;
      long currentStep = Math.min(writeRateRampUpSteps, elapsedMs / stepDurationMs);

      if (currentStep > lastComputedStep) {
        lastComputedStep = currentStep;
        double rateRange = maxWriteRatePerWorker - initialWriteRatePerWorker;
        double newRate =
            initialWriteRatePerWorker + (rateRange * currentStep) / (double) writeRateRampUpSteps;

        double oldRate = rateLimiter.getRate();
        if (newRate != oldRate) {
          rateLimiter.setRate(newRate);
          LOG.info(
              "Linear write rate ramp-up: increased write rate from {} to {} docs/s/worker"
                  + " (step {}/{}, elapsedMinutes={})",
              String.format("%.1f", oldRate),
              String.format("%.1f", newRate),
              currentStep,
              writeRateRampUpSteps,
              TimeUnit.MILLISECONDS.toMinutes(elapsedMs));
        }
      }
    }

    @Teardown
    public void teardown() {
      if (executor != null) {
        executor.shutdown();
      }
      if (mongoClient != null) {
        try {
          mongoClient.close();
        } catch (Exception ignored) {
        }
      }
    }

    @StartBundle
    public void startBundle() {
      if (mongoClient == null) {
        mongoClient = clientFactory.apply(uri);
      }
      futures = new ConcurrentLinkedQueue<>();
      failures = new ConcurrentLinkedQueue<>();
      successfulCount = new AtomicLong(0);
      dynamicCounters = new ConcurrentHashMap<>();
      inMemoryRetriesCount = new AtomicLong(0);
      severeFailedWritesCount = new AtomicLong(0);
      dlqRetriesCount = new AtomicLong(0);
      permanentFailuresCount = new AtomicLong(0);
      currentBatch = new ArrayList<>();
      LOG.debug("Starting new write bundle session (URI: {})", UriSanitizer.sanitize(uri));
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws InterruptedException {
      currentBatch.add(c.element());
      if (currentBatch.size() >= batchSize) {
        flushBatch();
      }
      DocumentWithMetadata failure;
      while ((failure = failures.poll()) != null) {
        c.output(failureTag, failure);
      }
    }

    private void flushBatch() throws InterruptedException {
      if (currentBatch.isEmpty()) {
        return;
      }
      List<DocumentWithMetadata> items = currentBatch;
      currentBatch = new ArrayList<>();

      Map<String, List<WriteModel<Document>>> updatesByCollection = new HashMap<>();
      Map<String, List<DocumentWithMetadata>> itemsByCollection = new HashMap<>();

      for (DocumentWithMetadata item : items) {
        String targetCol = item.getTargetCollection();
        if (targetCol == null) {
          targetCol = item.getSourceCollection();
        }

        if (item.getOperationType() != null && item.getOperationType().isDelete()) {
          Object id = item.getId();
          if (id != null) {
            updatesByCollection
                .computeIfAbsent(targetCol, k -> new ArrayList<>())
                .add(new DeleteOneModel<>(new Document("_id", id)));
            itemsByCollection.computeIfAbsent(targetCol, k -> new ArrayList<>()).add(item);
            writeDeletes.inc();
          } else {
            LOG.warn("Received DELETE event with null ID; routing to DLQ.");
            writePermanentDlqMessage(
                Collections.singletonList(item), "Received DELETE event with null ID");
          }
        } else if (item.getOperationType() != null
            && (item.getOperationType() == OperationType.DROP
                || item.getOperationType() == OperationType.RENAME)) {
          writeDropsSkipped.inc();
          LOG.info(
              "Received collection-level event '{}' for collection '{}';"
                  + " skipping document-level write.",
              item.getOperationType(),
              targetCol);
        } else {
          Document doc = item.getDocument();
          if (doc != null) {
            Object id = doc.get("_id");
            if (id != null) {
              updatesByCollection
                  .computeIfAbsent(targetCol, k -> new ArrayList<>())
                  .add(
                      new ReplaceOneModel<>(
                          new Document("_id", id), doc, new ReplaceOptions().upsert(true)));
              itemsByCollection.computeIfAbsent(targetCol, k -> new ArrayList<>()).add(item);
              writeInsertsUpserts.inc();
            } else {
              LOG.warn("Received document without '_id' field; routing to DLQ.");
              writePermanentDlqMessage(
                  Collections.singletonList(item), "Received document without '_id' field");
            }
          } else {
            LOG.warn(
                "Received upsert event with null document payload for key '{}'; routing to DLQ.",
                item.getDedupKey());
            writePermanentDlqMessage(
                Collections.singletonList(item),
                "Received upsert event with null document payload");
          }
        }
      }

      if (!updatesByCollection.isEmpty()) {
        batchesFlushed.inc();
        updateRateLimiterIfNeeded();
        if (rateLimiter != null && !items.isEmpty()) {
          rateLimiter.acquire(items.size());
        }
        LOG.debug(
            "Flushing batch of {} documents across {} target collection(s) to MongoDB (active"
                + " async write futures in queue: {})",
            items.size(),
            updatesByCollection.size(),
            futures.size());
        semaphore.acquire();
        CompletableFuture<Void> future =
            CompletableFuture.runAsync(
                () -> {
                  try {
                    for (Map.Entry<String, List<WriteModel<Document>>> entry :
                        updatesByCollection.entrySet()) {
                      String colName = entry.getKey();
                      List<WriteModel<Document>> currentUpdates = entry.getValue();
                      List<DocumentWithMetadata> currentItemList = itemsByCollection.get(colName);

                      MongoCollection<Document> col =
                          mongoClient.getDatabase(database).getCollection(colName);

                      writeBatchWithRetry(colName, col, currentUpdates, currentItemList);
                    }
                  } finally {
                    semaphore.release();
                  }
                },
                executor);
        futures.add(future);
      }
    }

    private void writeBatchWithRetry(
        String colName,
        MongoCollection<Document> col,
        List<WriteModel<Document>> currentUpdates,
        List<DocumentWithMetadata> currentItemList) {
      BackOff backoff = backoffSpec.backoff();
      Sleeper sleeper = Sleeper.DEFAULT;

      while (true) {
        try {
          col.bulkWrite(currentUpdates, new BulkWriteOptions().ordered(false));
          successfulCount.addAndGet(currentItemList.size());
          LOG.debug(
              "Successfully bulk-wrote {} documents to collection '{}'",
              currentItemList.size(),
              colName);
          break;
        } catch (MongoBulkWriteException e) {
          List<BulkWriteError> writeErrors = e.getWriteErrors();
          WriteConcernError wcError = e.getWriteConcernError();

          if (wcError != null && (writeErrors == null || writeErrors.isEmpty())) {
            LOG.warn(
                "Write concern failure on collection '{}' (code={}, message={}). Retrying all {}"
                    + " documents after backoff",
                colName,
                wcError.getCode(),
                wcError.getMessage(),
                currentItemList.size());
            incDynamicCounter(
                "inMemoryRetries",
                "MongoBulkWriteException_WriteConcern",
                wcError.getCode(),
                currentItemList.size());
            if (inMemoryRetriesCount != null) {
              inMemoryRetriesCount.addAndGet(currentItemList.size());
            }
            if (handleBackoff(sleeper, backoff, currentItemList)) {
              break;
            }
            continue;
          }

          successfulCount.addAndGet(currentItemList.size() - writeErrors.size());
          LOG.warn(
              "Transient MongoBulkWriteException on collection '{}' (errors={}). Retrying {}"
                  + " documents after backoff",
              colName,
              writeErrors.size(),
              writeErrors.size());

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
          int code = 0;
          if (e instanceof MongoException me) {
            code = me.getCode();
          }
          if (!isRetriable(e)) {
            incDynamicCounter(
                "severeFailedWrites", e.getClass().getSimpleName(), code, currentItemList.size());
            if (severeFailedWritesCount != null) {
              severeFailedWritesCount.addAndGet(currentItemList.size());
            }
            LOG.error(
                "Permanent write failure on collection '{}' (code={}): {}",
                colName,
                code,
                e.getMessage());
            writePermanentDlqMessage(
                currentItemList, "Failed to write documents: " + e.getMessage());
            break;
          }

          incDynamicCounter(
              "inMemoryRetries", e.getClass().getSimpleName(), code, currentItemList.size());
          if (inMemoryRetriesCount != null) {
            inMemoryRetriesCount.addAndGet(currentItemList.size());
          }
          LOG.warn(
              "Transient write exception on collection '{}': {}. Retrying {} documents after"
                  + " backoff",
              colName,
              e.getMessage(),
              currentItemList.size());
          if (handleBackoff(sleeper, backoff, currentItemList)) {
            break;
          }
        }
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
            incDynamicCounter("severeFailedWrites", "MongoBulkWriteException", error.getCode(), 1);
            if (severeFailedWritesCount != null) {
              severeFailedWritesCount.addAndGet(1);
            }
            writePermanentDlqMessage(
                Collections.singletonList(failedItem),
                "Permanent failure writing document. Error: " + error.getMessage());
          } else {
            incDynamicCounter("inMemoryRetries", "MongoBulkWriteException", error.getCode(), 1);
            if (inMemoryRetriesCount != null) {
              inMemoryRetriesCount.addAndGet(1);
            }
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
      if (isPermanent) {
        if (permanentFailuresCount != null) {
          permanentFailuresCount.addAndGet(itemList.size());
        }
      } else {
        if (dlqRetriesCount != null) {
          dlqRetriesCount.addAndGet(itemList.size());
        }
      }
      long now = System.currentTimeMillis();
      if (now - lastDlqLogTimeMs >= DLQ_LOG_INTERVAL_MS || lastDlqLogTimeMs == 0L) {
        lastDlqLogTimeMs = now;
        String sampleId = !itemList.isEmpty() ? String.valueOf(itemList.get(0).getId()) : "N/A";
        LOG.warn(
            "DLQ Error Summary (logged at most once every 30s per worker thread): {} document(s)"
                + " sent to DLQ in this batch. Reason: {} [Sample Doc ID: {}]",
            itemList.size(),
            message,
            sampleId);
      }
      for (DocumentWithMetadata item : itemList) {
        int nextRetryCount = (item.getRetryCount() != null ? item.getRetryCount() : 0) + 1;
        boolean isPerm =
            isPermanent || (dlqMaxRetries != null && nextRetryCount > dlqMaxRetries);
        int retryCount =
            isPerm
                ? Math.max(nextRetryCount, dlqMaxRetries != null ? dlqMaxRetries + 1 : 1)
                : nextRetryCount;
        ErrorType errorType = isPerm ? PERMANENT : RETRYABLE;

        failures.add(
            item.withFailure(
                message,
                errorType,
                FailureStage.WRITE,
                retryCount));
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
      try {
        flushBatch();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while flushing batch", e);
      }

      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

      successfulWrites.inc(successfulCount.get());
      if (inMemoryRetriesCount != null) {
        inMemoryRetries.inc(inMemoryRetriesCount.get());
      }
      if (severeFailedWritesCount != null) {
        severeFailedWrites.inc(severeFailedWritesCount.get());
      }
      if (dlqRetriesCount != null) {
        dlqRetries.inc(dlqRetriesCount.get());
      }
      if (permanentFailuresCount != null) {
        permanentFailures.inc(permanentFailuresCount.get());
      }
      if (dynamicCounters != null) {
        dynamicCounters.forEach(
            (name, count) -> Metrics.counter(WriteWithDlq.class, name).inc(count.get()));
      }

      DocumentWithMetadata failure;
      while ((failure = failures.poll()) != null) {
        c.output(failureTag, failure, Instant.now(), GlobalWindow.INSTANCE);
      }

      long succ = successfulCount.get();
      long memRetries = inMemoryRetriesCount != null ? inMemoryRetriesCount.get() : 0;
      long dlqRet = dlqRetriesCount != null ? dlqRetriesCount.get() : 0;
      long permFail = permanentFailuresCount != null ? permanentFailuresCount.get() : 0;
      if (succ > 0 || memRetries > 0 || dlqRet > 0 || permFail > 0) {
        LOG.info(
            "Finished write bundle: {} successful writes, {} in-memory retries, {} DLQ retries, {}"
                + " permanent failures",
            succ,
            memRetries,
            dlqRet,
            permFail);
      }
    }
  }

  /** A {@link DoFn} that writes grouped batches of documents to MongoDB in bulk. */
  public static class WriteBatchesFn
      extends DoFn<KV<String, Iterable<DocumentWithMetadata>>, DocumentWithMetadata> {

    private static final int ERR_DOCUMENT_VALIDATION_FAILURE = 121;
    private static final int ERR_KEY_TOO_LONG = 17280;
    private static final int ERR_BAD_VALUE = 2;
    private static final long DLQ_LOG_INTERVAL_MS = 30_000L;

    private static final Logger LOG = LoggerFactory.getLogger(WriteBatchesFn.class);

    private final String uri;
    private final String database;
    private final Integer maxWriteRetries;
    private final Integer dlqMaxRetries;
    private final Integer initialWriteRatePerWorker;
    private final Integer writeRateRampUpMinutes;
    private final Integer writeRateRampUpSteps;
    private final Integer maxWriteRatePerWorker;
    private final SerializableFunction<String, MongoClient> clientFactory;
    private final TupleTag<DocumentWithMetadata> failureTag;
    private transient FluentBackoff backoffSpec;
    private transient RateLimiter rateLimiter;
    private transient long startTimeMs;
    private transient long lastComputedStep;

    private final Counter successfulWrites =
        Metrics.counter(WriteWithDlq.class, "successfulWrites");
    private final Counter inMemoryRetries = Metrics.counter(WriteWithDlq.class, "inMemoryRetries");
    private final Counter severeFailedWrites =
        Metrics.counter(WriteWithDlq.class, "severeFailedWrites");
    private final Counter dlqRetries = Metrics.counter(WriteWithDlq.class, "dlqRetries");
    private final Counter permanentFailures =
        Metrics.counter(WriteWithDlq.class, "permanentFailures");
    private final Counter batchesFlushed =
        Metrics.counter(WriteWithDlq.class, "batchesFlushed");
    private final Counter writeInsertsUpserts =
        Metrics.counter(WriteWithDlq.class, "writeInsertsUpserts");
    private final Counter writeDeletes =
        Metrics.counter(WriteWithDlq.class, "writeDeletes");
    private final Counter writeDropsSkipped =
        Metrics.counter(WriteWithDlq.class, "writeDropsSkipped");
    private final Counter writeBatchesCoalesced =
        Metrics.counter(WriteWithDlq.class, "writeBatchesCoalesced");

    private transient MongoClient mongoClient;
    private transient ConcurrentLinkedQueue<DocumentWithMetadata> failures;
    private transient AtomicLong successfulCount;
    private transient ConcurrentHashMap<String, AtomicLong> dynamicCounters;
    private transient AtomicLong inMemoryRetriesCount;
    private transient AtomicLong severeFailedWritesCount;
    private transient AtomicLong dlqRetriesCount;
    private transient AtomicLong permanentFailuresCount;
    private transient long lastDlqLogTimeMs;

    private void incDynamicCounter(String prefix, String exceptionName, int code, long count) {
      String counterName = prefix + "_" + exceptionName + "_" + code;
      if (dynamicCounters != null) {
        dynamicCounters.computeIfAbsent(counterName, k -> new AtomicLong(0)).addAndGet(count);
      }
    }

    public WriteBatchesFn(
        String uri,
        String database,
        Integer maxConcurrentAsyncWrites,
        Integer maxWriteRetries,
        Integer dlqMaxRetries,
        Integer initialWriteRatePerWorker,
        Integer writeRateRampUpMinutes,
        Integer writeRateRampUpSteps,
        Integer maxWriteRatePerWorker,
        SerializableFunction<String, MongoClient> clientFactory,
        TupleTag<DocumentWithMetadata> failureTag) {
      this.uri = uri;
      this.database = database;
      this.maxWriteRetries = maxWriteRetries;
      this.dlqMaxRetries = dlqMaxRetries;
      this.initialWriteRatePerWorker = initialWriteRatePerWorker;
      this.writeRateRampUpMinutes = writeRateRampUpMinutes;
      this.writeRateRampUpSteps = writeRateRampUpSteps;
      this.maxWriteRatePerWorker = maxWriteRatePerWorker;
      this.clientFactory = clientFactory;
      this.failureTag = failureTag;
    }

    public static Builder builder() {
      return new Builder();
    }

    public static class Builder {
      private String uri;
      private String database;
      private Integer maxConcurrentAsyncWrites = 10;
      private Integer maxWriteRetries = 3;
      private Integer dlqMaxRetries = 3;
      private Integer initialWriteRatePerWorker = 5000;
      private Integer writeRateRampUpMinutes = 5;
      private Integer writeRateRampUpSteps = 5;
      private Integer maxWriteRatePerWorker = 25000;
      private SerializableFunction<String, MongoClient> clientFactory;
      private TupleTag<DocumentWithMetadata> failureTag;

      public Builder withUri(String uri) {
        this.uri = uri;
        return this;
      }

      public Builder withDatabase(String database) {
        this.database = database;
        return this;
      }

      public Builder withMaxConcurrentAsyncWrites(Integer maxConcurrentAsyncWrites) {
        if (maxConcurrentAsyncWrites != null) {
          this.maxConcurrentAsyncWrites = maxConcurrentAsyncWrites;
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

      public Builder withClientFactory(SerializableFunction<String, MongoClient> clientFactory) {
        this.clientFactory = clientFactory;
        return this;
      }

      public Builder withFailureTag(TupleTag<DocumentWithMetadata> failureTag) {
        this.failureTag = failureTag;
        return this;
      }

      public WriteBatchesFn build() {
        return new WriteBatchesFn(
            uri,
            database,
            maxConcurrentAsyncWrites,
            maxWriteRetries,
            dlqMaxRetries,
            initialWriteRatePerWorker,
            writeRateRampUpMinutes,
            writeRateRampUpSteps,
            maxWriteRatePerWorker,
            clientFactory,
            failureTag);
      }
    }

    @VisibleForTesting
    RateLimiter getRateLimiter() {
      return rateLimiter;
    }

    @VisibleForTesting
    void setStartTimeMs(long startTimeMs) {
      this.startTimeMs = startTimeMs;
    }

    @VisibleForTesting
    void updateRateLimiterForTest() {
      updateRateLimiterIfNeeded();
    }

    @Setup
    public void setup() {
      if (clientFactory != null && uri != null) {
        mongoClient = clientFactory.apply(uri);
      }
      backoffSpec =
          FluentBackoff.DEFAULT
              .withMaxRetries(maxWriteRetries)
              .withInitialBackoff(Duration.standardSeconds(2))
              .withExponent(2.0);
      if (initialWriteRatePerWorker != null && initialWriteRatePerWorker > 0) {
        rateLimiter = RateLimiter.create(initialWriteRatePerWorker);
        startTimeMs = System.currentTimeMillis();
        lastComputedStep = 0;
        LOG.info(
            "Enabled linear write rate ramp-up for WriteBatchesFn: initialRate={} docs/s/worker,"
                + " targetMax={} docs/s/worker, duration={} mins, steps={}",
            initialWriteRatePerWorker,
            maxWriteRatePerWorker,
            writeRateRampUpMinutes,
            writeRateRampUpSteps);
      } else {
        rateLimiter = null;
        LOG.info(
            "Write rate limiting is disabled for WriteBatchesFn (initialWriteRatePerWorker <= 0)");
      }
      LOG.info(
          "Initialized MongoDB WriteBatchesFn worker thread for database '{}' (maxWriteRetries={})",
          database,
          maxWriteRetries);
    }

    private void updateRateLimiterIfNeeded() {
      if (rateLimiter == null
          || writeRateRampUpMinutes == null
          || writeRateRampUpMinutes <= 0
          || writeRateRampUpSteps == null
          || writeRateRampUpSteps <= 0
          || maxWriteRatePerWorker == null
          || maxWriteRatePerWorker <= initialWriteRatePerWorker) {
        return;
      }
      long stepDurationMs = (writeRateRampUpMinutes * 60L * 1000L) / writeRateRampUpSteps;
      if (stepDurationMs <= 0) {
        stepDurationMs = 1;
      }
      long elapsedMs = System.currentTimeMillis() - startTimeMs;
      long currentStep = Math.min(writeRateRampUpSteps, elapsedMs / stepDurationMs);

      if (currentStep > lastComputedStep) {
        lastComputedStep = currentStep;
        double rateRange = maxWriteRatePerWorker - initialWriteRatePerWorker;
        double newRate =
            initialWriteRatePerWorker + (rateRange * currentStep) / (double) writeRateRampUpSteps;

        double oldRate = rateLimiter.getRate();
        if (newRate != oldRate) {
          rateLimiter.setRate(newRate);
          LOG.info(
              "Linear write rate ramp-up: increased write rate from {} to {} docs/s/worker"
                  + " (step {}/{}, elapsedMinutes={})",
              String.format("%.1f", oldRate),
              String.format("%.1f", newRate),
              currentStep,
              writeRateRampUpSteps,
              TimeUnit.MILLISECONDS.toMinutes(elapsedMs));
        }
      }
    }

    @Teardown
    public void teardown() {
      if (mongoClient != null) {
        try {
          mongoClient.close();
        } catch (Exception ignored) {
        }
      }
    }

    @StartBundle
    public void startBundle() {
      if (mongoClient == null && clientFactory != null && uri != null) {
        mongoClient = clientFactory.apply(uri);
      }
      failures = new ConcurrentLinkedQueue<>();
      successfulCount = new AtomicLong(0);
      dynamicCounters = new ConcurrentHashMap<>();
      inMemoryRetriesCount = new AtomicLong(0);
      severeFailedWritesCount = new AtomicLong(0);
      dlqRetriesCount = new AtomicLong(0);
      permanentFailuresCount = new AtomicLong(0);
      LOG.debug("Starting new WriteBatchesFn bundle session (URI: {})", UriSanitizer.sanitize(uri));
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws InterruptedException {
      KV<String, Iterable<DocumentWithMetadata>> element = c.element();
      if (element != null && element.getValue() != null) {
        List<DocumentWithMetadata> items = new ArrayList<>();
        for (DocumentWithMetadata item : element.getValue()) {
          if (item != null) {
            items.add(item);
          }
        }
        if (!items.isEmpty()) {
          flushBatch(items);
        }
      }
      DocumentWithMetadata failure;
      while ((failure = failures.poll()) != null) {
        c.output(failureTag, failure);
      }
    }

    private static class CoalescedOp {
      final WriteModel<Document> model;
      final DocumentWithMetadata item;
      final boolean isDelete;

      CoalescedOp(WriteModel<Document> model, DocumentWithMetadata item, boolean isDelete) {
        this.model = model;
        this.item = item;
        this.isDelete = isDelete;
      }
    }

    private void flushBatch(List<DocumentWithMetadata> items) throws InterruptedException {
      if (items == null || items.isEmpty()) {
        return;
      }

      Map<String, LinkedHashMap<Object, CoalescedOp>> coalescedByCollection = new HashMap<>();

      for (DocumentWithMetadata item : items) {
        String targetCol = item.getTargetCollection();
        if (targetCol == null) {
          targetCol = item.getSourceCollection();
        }

        if (item.getOperationType() != null && item.getOperationType().isDelete()) {
          Object id = item.getId();
          if (id != null) {
            LinkedHashMap<Object, CoalescedOp> colMap =
                coalescedByCollection.computeIfAbsent(targetCol, k -> new LinkedHashMap<>());
            CoalescedOp prev =
                colMap.put(
                    id,
                    new CoalescedOp(
                        new DeleteOneModel<>(new Document("_id", id)), item, true));
            if (prev != null) {
              writeBatchesCoalesced.inc();
            }
          } else {
            LOG.warn("Received DELETE event with null ID; routing to DLQ.");
            writePermanentDlqMessage(
                Collections.singletonList(item), "Received DELETE event with null ID");
          }
        } else if (item.getOperationType() != null
            && (item.getOperationType() == OperationType.DROP
                || item.getOperationType() == OperationType.RENAME)) {
          writeDropsSkipped.inc();
          LOG.info(
              "Received collection-level event '{}' for collection '{}';"
                  + " skipping document-level write.",
              item.getOperationType(),
              targetCol);
        } else {
          Document doc = item.getDocument();
          if (doc != null) {
            Object id = doc.get("_id");
            if (id != null) {
              LinkedHashMap<Object, CoalescedOp> colMap =
                  coalescedByCollection.computeIfAbsent(targetCol, k -> new LinkedHashMap<>());
              CoalescedOp prev =
                  colMap.put(
                      id,
                      new CoalescedOp(
                          new ReplaceOneModel<>(
                              new Document("_id", id), doc, new ReplaceOptions().upsert(true)),
                          item,
                          false));
              if (prev != null) {
                writeBatchesCoalesced.inc();
              }
            } else {
              LOG.warn("Received document without '_id' field; routing to DLQ.");
              writePermanentDlqMessage(
                  Collections.singletonList(item), "Received document without '_id' field");
            }
          } else {
            LOG.warn(
                "Received upsert event with null document payload for key '{}'; routing to DLQ.",
                item.getDedupKey());
            writePermanentDlqMessage(
                Collections.singletonList(item),
                "Received upsert event with null document payload");
          }
        }
      }

      if (!coalescedByCollection.isEmpty()) {
        batchesFlushed.inc();
        updateRateLimiterIfNeeded();

        int totalCoalescedCount = 0;
        for (LinkedHashMap<Object, CoalescedOp> colMap : coalescedByCollection.values()) {
          totalCoalescedCount += colMap.size();
        }

        if (rateLimiter != null && totalCoalescedCount > 0) {
          rateLimiter.acquire(totalCoalescedCount);
        }

        LOG.debug(
            "Flushing coalesced batch of {} documents (from {} input items) across {}"
                + " target collection(s) to MongoDB",
            totalCoalescedCount,
            items.size(),
            coalescedByCollection.size());

        for (Map.Entry<String, LinkedHashMap<Object, CoalescedOp>> entry :
            coalescedByCollection.entrySet()) {
          String colName = entry.getKey();
          LinkedHashMap<Object, CoalescedOp> colMap = entry.getValue();

          List<WriteModel<Document>> currentUpdates = new ArrayList<>(colMap.size());
          List<DocumentWithMetadata> currentItemList = new ArrayList<>(colMap.size());

          for (CoalescedOp op : colMap.values()) {
            currentUpdates.add(op.model);
            currentItemList.add(op.item);
            if (op.isDelete) {
              writeDeletes.inc();
            } else {
              writeInsertsUpserts.inc();
            }
          }

          MongoCollection<Document> col =
              mongoClient.getDatabase(database).getCollection(colName);

          writeBatchWithRetry(colName, col, currentUpdates, currentItemList);
        }
      }
    }

    private void writeBatchWithRetry(
        String colName,
        MongoCollection<Document> col,
        List<WriteModel<Document>> currentUpdates,
        List<DocumentWithMetadata> currentItemList) {
      BackOff backoff = backoffSpec.backoff();
      Sleeper sleeper = Sleeper.DEFAULT;

      while (true) {
        try {
          col.bulkWrite(currentUpdates, new BulkWriteOptions().ordered(false));
          successfulCount.addAndGet(currentItemList.size());
          LOG.debug(
              "Successfully bulk-wrote {} documents to collection '{}'",
              currentItemList.size(),
              colName);
          break;
        } catch (MongoBulkWriteException e) {
          List<BulkWriteError> writeErrors = e.getWriteErrors();
          WriteConcernError wcError = e.getWriteConcernError();

          if (wcError != null && (writeErrors == null || writeErrors.isEmpty())) {
            LOG.warn(
                "Write concern failure on collection '{}' (code={}, message={}). Retrying all {}"
                    + " documents after backoff",
                colName,
                wcError.getCode(),
                wcError.getMessage(),
                currentItemList.size());
            incDynamicCounter(
                "inMemoryRetries",
                "MongoBulkWriteException_WriteConcern",
                wcError.getCode(),
                currentItemList.size());
            if (inMemoryRetriesCount != null) {
              inMemoryRetriesCount.addAndGet(currentItemList.size());
            }
            if (handleBackoff(sleeper, backoff, currentItemList)) {
              break;
            }
            continue;
          }

          successfulCount.addAndGet(currentItemList.size() - writeErrors.size());
          LOG.warn(
              "Transient MongoBulkWriteException on collection '{}' (errors={}). Retrying {}"
                  + " documents after backoff",
              colName,
              writeErrors.size(),
              writeErrors.size());

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
          int code = 0;
          if (e instanceof MongoException me) {
            code = me.getCode();
          }
          if (!isRetriable(e)) {
            incDynamicCounter(
                "severeFailedWrites", e.getClass().getSimpleName(), code, currentItemList.size());
            if (severeFailedWritesCount != null) {
              severeFailedWritesCount.addAndGet(currentItemList.size());
            }
            LOG.error(
                "Permanent write failure on collection '{}' (code={}): {}",
                colName,
                code,
                e.getMessage());
            writePermanentDlqMessage(
                currentItemList, "Failed to write documents: " + e.getMessage());
            break;
          }

          incDynamicCounter(
              "inMemoryRetries", e.getClass().getSimpleName(), code, currentItemList.size());
          if (inMemoryRetriesCount != null) {
            inMemoryRetriesCount.addAndGet(currentItemList.size());
          }
          LOG.warn(
              "Transient write exception on collection '{}': {}. Retrying {} documents after"
                  + " backoff",
              colName,
              e.getMessage(),
              currentItemList.size());
          if (handleBackoff(sleeper, backoff, currentItemList)) {
            break;
          }
        }
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
            incDynamicCounter("severeFailedWrites", "MongoBulkWriteException", error.getCode(), 1);
            if (severeFailedWritesCount != null) {
              severeFailedWritesCount.addAndGet(1);
            }
            writePermanentDlqMessage(
                Collections.singletonList(failedItem),
                "Permanent failure writing document. Error: " + error.getMessage());
          } else {
            incDynamicCounter("inMemoryRetries", "MongoBulkWriteException", error.getCode(), 1);
            if (inMemoryRetriesCount != null) {
              inMemoryRetriesCount.addAndGet(1);
            }
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
      if (isPermanent) {
        if (permanentFailuresCount != null) {
          permanentFailuresCount.addAndGet(itemList.size());
        }
      } else {
        if (dlqRetriesCount != null) {
          dlqRetriesCount.addAndGet(itemList.size());
        }
      }
      long now = System.currentTimeMillis();
      if (now - lastDlqLogTimeMs >= DLQ_LOG_INTERVAL_MS || lastDlqLogTimeMs == 0L) {
        lastDlqLogTimeMs = now;
        String sampleId = !itemList.isEmpty() ? String.valueOf(itemList.get(0).getId()) : "N/A";
        LOG.warn(
            "DLQ Error Summary (logged at most once every 30s per worker thread): {} document(s)"
                + " sent to DLQ in this batch. Reason: {} [Sample Doc ID: {}]",
            itemList.size(),
            message,
            sampleId);
      }
      for (DocumentWithMetadata item : itemList) {
        int nextRetryCount = (item.getRetryCount() != null ? item.getRetryCount() : 0) + 1;
        boolean isPerm =
            isPermanent || (dlqMaxRetries != null && nextRetryCount > dlqMaxRetries);
        int retryCount =
            isPerm
                ? Math.max(nextRetryCount, dlqMaxRetries != null ? dlqMaxRetries + 1 : 1)
                : nextRetryCount;
        ErrorType errorType = isPerm ? PERMANENT : RETRYABLE;

        failures.add(
            item.withFailure(
                message,
                errorType,
                FailureStage.WRITE,
                retryCount));
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
      successfulWrites.inc(successfulCount.get());
      if (inMemoryRetriesCount != null) {
        inMemoryRetries.inc(inMemoryRetriesCount.get());
      }
      if (severeFailedWritesCount != null) {
        severeFailedWrites.inc(severeFailedWritesCount.get());
      }
      if (dlqRetriesCount != null) {
        dlqRetries.inc(dlqRetriesCount.get());
      }
      if (permanentFailuresCount != null) {
        permanentFailures.inc(permanentFailuresCount.get());
      }
      if (dynamicCounters != null) {
        dynamicCounters.forEach(
            (name, count) -> Metrics.counter(WriteWithDlq.class, name).inc(count.get()));
      }

      DocumentWithMetadata failure;
      while ((failure = failures.poll()) != null) {
        c.output(failureTag, failure, Instant.now(), GlobalWindow.INSTANCE);
      }

      long succ = successfulCount.get();
      long memRetries = inMemoryRetriesCount != null ? inMemoryRetriesCount.get() : 0;
      long dlqRet = dlqRetriesCount != null ? dlqRetriesCount.get() : 0;
      long permFail = permanentFailuresCount != null ? permanentFailuresCount.get() : 0;
      if (succ > 0 || memRetries > 0 || dlqRet > 0 || permFail > 0) {
        LOG.info(
            "Finished WriteBatchesFn bundle: {} successful writes, {} in-memory retries,"
                + " {} DLQ retries, {} permanent failures",
            succ,
            memRetries,
            dlqRet,
            permFail);
      }
    }
  }

  /** A {@link DoFn} that applies a JavaScript UDF to the document. */
  public static class ApplyUdfFn extends DoFn<DocumentWithMetadata, DocumentWithMetadata> {
    private static final Logger LOG = LoggerFactory.getLogger(ApplyUdfFn.class);
    private static final JsonWriterSettings CANONICAL_JSON_SETTINGS =
        JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

    private final String fileSystemPath;
    private final String functionName;
    private final Integer reloadIntervalMinutes;
    private final TupleTag<DocumentWithMetadata> failureTag;
    private transient JavascriptRuntime javascriptRuntime;
    private final Counter udfProcessingFailures =
        Metrics.counter(ApplyUdfFn.class, "udfProcessingFailures");

    public ApplyUdfFn(
        String fileSystemPath,
        String functionName,
        Integer reloadIntervalMinutes,
        TupleTag<DocumentWithMetadata> failureTag) {
      this.fileSystemPath = fileSystemPath;
      this.functionName = functionName;
      this.reloadIntervalMinutes = reloadIntervalMinutes;
      this.failureTag = failureTag;
    }

    @Setup
    public void setup() {
      if (fileSystemPath != null && functionName != null) {
        javascriptRuntime =
            JavascriptRuntime.newBuilder()
                .setFileSystemPath(fileSystemPath)
                .setFunctionName(functionName)
                .setReloadIntervalMinutes(reloadIntervalMinutes)
                .build();
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      DocumentWithMetadata item = c.element();
      if (item.getOperationType() != null && !item.getOperationType().isUpsert()) {
        c.output(item);
        return;
      }
      if (javascriptRuntime != null) {
        try {
          String payload = item.getOriginalDocument();
          if (payload == null && item.getDocument() != null) {
            payload = item.getDocument().toJson(CANONICAL_JSON_SETTINGS);
          }
          if (payload == null) {
            c.output(item);
            return;
          }
          String transformed = javascriptRuntime.invoke(payload);
          if (transformed != null) {
            Document doc = Document.parse(transformed);
            c.output(item.withDocument(doc));
          } else {
            LOG.warn("UDF returned null for document ID: {}", item.getId());
            udfProcessingFailures.inc();
            c.output(
                failureTag,
                item.withFailure(
                    "UDF returned null",
                    RETRYABLE,
                    FailureStage.UDF));
          }
        } catch (Throwable e) {
          LOG.error("Failed to apply UDF: {}", e.getMessage());
          udfProcessingFailures.inc();
          c.output(
              failureTag,
              item.withFailure(
                  "UDF failed: " + e.getMessage(),
                  RETRYABLE,
                  FailureStage.UDF));
        }
      } else {
        c.output(item);
      }
    }
  }
}
