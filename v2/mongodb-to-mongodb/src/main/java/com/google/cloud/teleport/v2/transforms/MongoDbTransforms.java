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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
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
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.types.Binary;
import org.bson.types.CodeWithScope;
import org.bson.types.Symbol;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transforms for the MongoDB to MongoDB template. */
public class MongoDbTransforms {

  private static final ConcurrentHashMap<String, MongoClient> CLIENT_CACHE =
      new ConcurrentHashMap<>();

  private static final ConcurrentHashMap<Integer, SharedWritePool> WRITE_POOL_CACHE =
      new ConcurrentHashMap<>();

  private static final ConcurrentHashMap<String, WorkerRateLimiter> WORKER_RATE_LIMITERS =
      new ConcurrentHashMap<>();

  /**
   * Per-worker-JVM write rate limiter shared across all harness threads of a {@link DoFn} stage.
   *
   * <p>When {@code maxRate > 0}, linearly increases the per-worker write rate from {@code
   * initialRate} to {@code maxRate} over {@code rampUpMinutes} across {@code rampUpSteps} steps.
   * When {@code maxRate == null || maxRate <= 0}, warms up exponentially ({@code initialRate *
   * 2^elapsedMinutes}) over {@code rampUpMinutes} across {@code rampUpSteps} steps and then
   * disables rate limiting completely so steady-state writes run uncapped with zero lock overhead.
   *
   * <p>The warm-up clock anchors lazily on the first {@link #acquire(int)} call so upstream split
   * generation and {@code GroupIntoBatches} buffering delays do not prematurely consume the warm-up
   * window.
   */
  static final class WorkerRateLimiter {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerRateLimiter.class);

    private final double initialRate;
    private final double maxRate;
    private final boolean uncappedAfterRampUp;
    private final long rampUpDurationMs;
    private final int rampUpSteps;
    private final ReentrantLock acquireLock = new ReentrantLock();

    private volatile boolean disabled;
    private volatile double currentRate;
    private long startTimeMs;
    private long lastComputedStep;
    private double nextFreeTimeMs;

    WorkerRateLimiter(
        int initialRate, Integer maxRate, Integer rampUpMinutes, Integer rampUpSteps) {
      this.uncappedAfterRampUp = (maxRate == null || maxRate <= 0);
      this.maxRate = this.uncappedAfterRampUp ? 0.0 : maxRate;
      this.initialRate =
          this.uncappedAfterRampUp ? initialRate : Math.min((double) initialRate, this.maxRate);
      int mins = (rampUpMinutes != null && rampUpMinutes > 0) ? rampUpMinutes : 0;
      this.rampUpDurationMs = mins * 60_000L;
      int defaultSteps = Math.max(1, mins);
      this.rampUpSteps = (rampUpSteps != null && rampUpSteps > 0) ? rampUpSteps : defaultSteps;
      this.startTimeMs = -1L;
      this.nextFreeTimeMs = 0.0;
      this.lastComputedStep = 0;
      if (this.uncappedAfterRampUp && this.rampUpDurationMs <= 0) {
        this.disabled = true;
        this.currentRate = Double.POSITIVE_INFINITY;
      } else {
        this.disabled = false;
        this.currentRate =
            (!this.uncappedAfterRampUp && this.rampUpDurationMs <= 0)
                ? this.maxRate
                : this.initialRate;
        LOG.info(
            "Enabled per-worker write rate {} ramp-up: initialRate={} docs/s/worker,"
                + " targetMax={} docs/s/worker, duration={} mins, steps={}",
            this.uncappedAfterRampUp ? "uncapped-exponential" : "bounded-linear",
            this.initialRate,
            this.uncappedAfterRampUp ? "UNCAPPED" : maxRate,
            mins,
            this.rampUpSteps);
      }
    }

    synchronized double updateRate(long nowMs) {
      if (disabled) {
        return Double.POSITIVE_INFINITY;
      }
      if (startTimeMs < 0 || rampUpDurationMs <= 0 || rampUpSteps <= 0) {
        return currentRate;
      }
      long elapsedMs = Math.max(0L, nowMs - startTimeMs);
      if (uncappedAfterRampUp && elapsedMs >= rampUpDurationMs) {
        disabled = true;
        currentRate = Double.POSITIVE_INFINITY;
        LOG.info(
            "Write rate warm-up complete after {} mins: disabling write rate limiter"
                + " (uncapped steady-state)",
            TimeUnit.MILLISECONDS.toMinutes(elapsedMs));
        return currentRate;
      }
      if (!uncappedAfterRampUp && maxRate <= initialRate) {
        return currentRate;
      }

      long stepDurationMs = Math.max(1L, rampUpDurationMs / rampUpSteps);
      long currentStep = Math.min(rampUpSteps, elapsedMs / stepDurationMs);
      if (currentStep > lastComputedStep) {
        lastComputedStep = currentStep;
        double oldRate = currentRate;
        double newRate;
        if (uncappedAfterRampUp) {
          double elapsedRampMinutes =
              (currentStep * (rampUpDurationMs / 60_000.0)) / (double) rampUpSteps;
          newRate = initialRate * Math.pow(2.0, elapsedRampMinutes);
        } else {
          double rateRange = maxRate - initialRate;
          newRate = initialRate + (rateRange * currentStep) / (double) rampUpSteps;
        }
        if (newRate != oldRate) {
          currentRate = newRate;
          LOG.info(
              "Write rate ramp-up ({}): increased write rate from {} to {} docs/s/worker"
                  + " (step {}/{}, elapsedMinutes={})",
              uncappedAfterRampUp ? "uncapped-exponential" : "bounded-linear",
              String.format("%.1f", oldRate),
              String.format("%.1f", newRate),
              currentStep,
              rampUpSteps,
              TimeUnit.MILLISECONDS.toMinutes(elapsedMs));
        }
      }
      return currentRate;
    }

    void acquire(int permits) throws InterruptedException {
      if (permits <= 0 || disabled) {
        return;
      }
      acquireLock.lockInterruptibly();
      try {
        while (!disabled) {
          long nowMs = System.currentTimeMillis();
          double waitMs;
          synchronized (this) {
            if (startTimeMs < 0) {
              startTimeMs = nowMs;
              nextFreeTimeMs = nowMs;
            }
            double rate = updateRate(nowMs);
            if (disabled || !Double.isFinite(rate) || rate <= 0) {
              return;
            }
            waitMs = nextFreeTimeMs - nowMs;
            if (waitMs <= 0) {
              double baseMs = Math.max(nextFreeTimeMs, nowMs - 1000.0);
              nextFreeTimeMs = baseMs + (permits * 1000.0) / rate;
              return;
            }
          }
          long sleepMs = Math.min((long) Math.ceil(waitMs), 100L);
          if (sleepMs > 0) {
            Thread.sleep(sleepMs);
          }
        }
      } finally {
        acquireLock.unlock();
      }
    }

    double getRate() {
      return currentRate;
    }

    boolean isDisabled() {
      return disabled;
    }

    synchronized long getStartTimeMs() {
      return startTimeMs;
    }

    synchronized void setStartTimeMs(long startTimeMs) {
      this.startTimeMs = startTimeMs;
      this.nextFreeTimeMs = startTimeMs;
      this.lastComputedStep = 0;
    }
  }

  private static WorkerRateLimiter getOrCreateWorkerRateLimiter(
      String key,
      Integer initialRate,
      Integer maxRate,
      Integer rampUpMinutes,
      Integer rampUpSteps) {
    if (initialRate == null || initialRate <= 0) {
      return null;
    }
    return WORKER_RATE_LIMITERS.computeIfAbsent(
        key, k -> new WorkerRateLimiter(initialRate, maxRate, rampUpMinutes, rampUpSteps));
  }

  private static class SharedWritePool {
    final ExecutorService executor;
    final Semaphore semaphore;

    SharedWritePool(int concurrency) {
      this.executor =
          Executors.newFixedThreadPool(
              concurrency,
              r -> {
                Thread t = new Thread(r, "mongodb-async-writer");
                t.setDaemon(true);
                return t;
              });
      this.semaphore = new Semaphore(concurrency);
    }
  }

  private static SharedWritePool getOrCreateWritePool(Integer maxConcurrentAsyncWrites) {
    int concurrency =
        (maxConcurrentAsyncWrites != null && maxConcurrentAsyncWrites > 0)
            ? maxConcurrentAsyncWrites
            : 10;
    return WRITE_POOL_CACHE.computeIfAbsent(concurrency, SharedWritePool::new);
  }

  /**
   * Per-bundle state container isolating async write futures, DLQ failures, and counters so an
   * aborted bundle's background tasks can quiesce safely before a subsequent bundle runs and can
   * never leak failures or metrics into a later bundle.
   */
  private static final class BundleWriteState {
    final ConcurrentLinkedQueue<CompletableFuture<Void>> futures = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<DocumentWithMetadata> failures = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<Long> writeLatenciesMs = new ConcurrentLinkedQueue<>();
    final AtomicLong successfulCount = new AtomicLong(0);
    final ConcurrentHashMap<String, AtomicLong> dynamicCounters = new ConcurrentHashMap<>();
    final AtomicLong inMemoryRetriesCount = new AtomicLong(0);
    final AtomicLong severeFailedWritesCount = new AtomicLong(0);
    final AtomicLong dlqRetriesCount = new AtomicLong(0);
    final AtomicLong permanentFailuresCount = new AtomicLong(0);

    void incDynamicCounter(String prefix, String exceptionName, int code, long count) {
      String counterName = prefix + "_" + exceptionName + "_" + code;
      dynamicCounters.computeIfAbsent(counterName, k -> new AtomicLong(0)).addAndGet(count);
    }

    void awaitQuiescence() {
      if (!futures.isEmpty()) {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
      }
    }

    void awaitQuiescenceQuietly() {
      for (CompletableFuture<Void> future : futures) {
        try {
          future.join();
        } catch (Throwable ignored) {
          // Wait only for in-flight RPCs of a prior aborted bundle to stop mutating MongoDB.
        }
      }
    }
  }

  /** Returns a cached JVM-wide MongoClient for the given URI, or creates a new one if absent. */
  public static MongoClient getOrCreateMongoClient(String uri) {
    return CLIENT_CACHE.computeIfAbsent(uri, MongoDbTransforms::createMongoClient);
  }

  /**
   * Closes the given {@link MongoClient} only if it is not a shared JVM-wide instance managed by
   * {@link #CLIENT_CACHE}. Shared clients remain open for the lifetime of the worker JVM so that
   * concurrent {@link DoFn} instances on the same worker do not close each other's connections.
   */
  public static void closeIfNotShared(MongoClient client) {
    if (client != null && !CLIENT_CACHE.containsValue(client)) {
      try {
        client.close();
      } catch (Exception ignored) {
        // Best-effort close during DoFn teardown.
      }
    }
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
        pool ->
            pool.maxSize(
                    Objects.requireNonNullElse(connectionString.getMaxConnectionPoolSize(), 256))
                .minSize(Objects.requireNonNullElse(connectionString.getMinConnectionPoolSize(), 0))
                .maxWaitTime(
                    Objects.requireNonNullElse(connectionString.getMaxWaitTime(), 30_000),
                    TimeUnit.MILLISECONDS));
    builder.applyToSocketSettings(
        socket ->
            socket
                .connectTimeout(
                    Objects.requireNonNullElse(connectionString.getConnectTimeout(), 15_000),
                    TimeUnit.MILLISECONDS)
                .readTimeout(
                    Objects.requireNonNullElse(connectionString.getSocketTimeout(), 60_000),
                    TimeUnit.MILLISECONDS));
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
        MongoDbTransforms::getOrCreateMongoClient;

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
                              dedupKey != null ? Math.floorMod(dedupKey.hashCode(), shards) : 0;
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
    // Allow DLQ windows clamped to Instant.now() to remain non-expired even when upstream
    // GroupIntoBatches flushes at GlobalWindow.INSTANCE.maxTimestamp() (+294247) during drain/EOF.
    private static final Duration DLQ_ALLOWED_LATENESS =
        Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
    private static final Duration DLQ_EARLY_FLUSH_DELAY = Duration.standardSeconds(10);
    private final String retryablePath;
    private final String permanentPath;

    @SuppressWarnings("unused")
    private final String tempLocation;

    private final Counter dlqRetryableEmitted =
        Metrics.counter(WriteToDlq.class, "dlqRetryableEmitted");
    private final Counter dlqPermanentEmitted =
        Metrics.counter(WriteToDlq.class, "dlqPermanentEmitted");

    public WriteToDlq(String retryablePath, String permanentPath, String tempLocation) {
      this(null, retryablePath, permanentPath, tempLocation);
    }

    public WriteToDlq(
        String stageName, String retryablePath, String permanentPath, String tempLocation) {
      super(stageName);
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

      // Dataflow Streaming Engine requires windowed writes with explicit sharding even when the
      // upstream PCollection is bounded (e.g., BACKFILL mode forced onto Streaming Engine).
      boolean isUnbounded =
          input.isBounded() == PCollection.IsBounded.UNBOUNDED
              || input.getPipeline().getOptions().as(StreamingOptions.class).isStreaming();

      String filePrefix =
          (getName() != null && !getName().isEmpty())
              ? "/error-" + getName().replaceAll("[^a-zA-Z0-9_-]", "").toLowerCase()
              : "/error";

      writeDlqTier(
          retryable, "Retryable", retryablePath + filePrefix, dlqRetryableEmitted, isUnbounded);
      writeDlqTier(
          permanent, "Permanent", permanentPath + filePrefix, dlqPermanentEmitted, isUnbounded);

      return PDone.in(input.getPipeline());
    }

    private static void writeDlqTier(
        PCollection<DocumentWithMetadata> tierItems,
        String tierName,
        String outputPrefix,
        Counter emittedCounter,
        boolean isUnbounded) {
      PCollection<String> jsonLines =
          tierItems.apply(
              "MapToJson_" + tierName,
              ParDo.of(
                  new DoFn<DocumentWithMetadata, String>() {
                    @Override
                    public Duration getAllowedTimestampSkew() {
                      return Duration.millis(Long.MAX_VALUE);
                    }

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      emittedCounter.inc();
                      DocumentWithMetadata item = c.element();
                      String json =
                          item.toDlqJson(
                              item.getErrorMessage(), item.getErrorType(), item.getRetryCount());
                      // Stamp every DLQ record with wall-clock Instant.now() so upstream failures
                      // carrying BoundedWindow.TIMESTAMP_MIN_VALUE (UDF, Validate, mid-bundle
                      // writes) or GlobalWindow.INSTANCE.maxTimestamp() (end-of-GlobalWindow
                      // flushes) land in valid wall-clock 1-minute windows without expiring.
                      c.outputWithTimestamp(json, Instant.now());
                    }
                  }));

      if (isUnbounded) {
        jsonLines
            .apply(
                "Window" + tierName + "Dlq",
                Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings(
                                AfterProcessingTime.pastFirstElementInPane()
                                    .plusDelayOf(DLQ_EARLY_FLUSH_DELAY))
                            .withLateFirings(AfterPane.elementCountAtLeast(1)))
                    .withAllowedLateness(DLQ_ALLOWED_LATENESS)
                    .discardingFiredPanes())
            .apply(
                "WriteDlq_" + tierName,
                TextIO.write()
                    .to(outputPrefix)
                    .withSuffix(".json")
                    .withWindowedWrites()
                    .withNumShards(1));
      } else {
        jsonLines.apply(
            "WriteDlq_" + tierName, TextIO.write().to(outputPrefix).withSuffix(".json"));
      }
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
    private final String rateLimiterKey = UUID.randomUUID().toString();
    private transient FluentBackoff backoffSpec;
    private transient WorkerRateLimiter workerRateLimiter;

    private final Counter successfulWrites =
        Metrics.counter(WriteWithDlq.class, "successfulWrites");
    private final Counter inMemoryRetries = Metrics.counter(WriteWithDlq.class, "inMemoryRetries");
    private final Counter severeFailedWrites =
        Metrics.counter(WriteWithDlq.class, "severeFailedWrites");
    private final Counter dlqRetries = Metrics.counter(WriteWithDlq.class, "dlqRetries");
    private final Counter permanentFailures =
        Metrics.counter(WriteWithDlq.class, "permanentFailures");
    private final Counter batchesFlushed = Metrics.counter(WriteWithDlq.class, "batchesFlushed");
    private final Counter writeInsertsUpserts =
        Metrics.counter(WriteWithDlq.class, "writeInsertsUpserts");
    private final Counter writeDeletes = Metrics.counter(WriteWithDlq.class, "writeDeletes");
    private final Counter writeDropsSkipped =
        Metrics.counter(WriteWithDlq.class, "writeDropsSkipped");

    private transient MongoClient mongoClient;
    private transient ExecutorService executor;
    private transient Semaphore semaphore;
    private transient BundleWriteState bundleState;
    private transient List<DocumentWithMetadata> currentBatch;
    private transient AtomicLong lastDlqLogTimeMs;

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
    WorkerRateLimiter getRateLimiter() {
      return (workerRateLimiter != null && !workerRateLimiter.isDisabled())
          ? workerRateLimiter
          : null;
    }

    @VisibleForTesting
    void setStartTimeMs(long startTimeMs) {
      if (workerRateLimiter != null) {
        workerRateLimiter.setStartTimeMs(startTimeMs);
      }
    }

    @VisibleForTesting
    void updateRateLimiterForTest() {
      if (workerRateLimiter != null) {
        workerRateLimiter.updateRate(System.currentTimeMillis());
      }
    }

    @Setup
    public void setup() {
      executor =
          Executors.newFixedThreadPool(
              maxConcurrentAsyncWrites,
              r -> {
                Thread t = new Thread(r, "mongodb-writefn-async");
                t.setDaemon(true);
                return t;
              });
      semaphore = new Semaphore(maxConcurrentAsyncWrites);
      lastDlqLogTimeMs = new AtomicLong(0L);
      if (clientFactory != null && uri != null) {
        mongoClient = clientFactory.apply(uri);
      }
      backoffSpec =
          FluentBackoff.DEFAULT
              .withMaxRetries(maxWriteRetries)
              .withInitialBackoff(Duration.millis(200))
              .withExponent(2.0);
      workerRateLimiter =
          getOrCreateWorkerRateLimiter(
              rateLimiterKey,
              initialWriteRatePerWorker,
              maxWriteRatePerWorker,
              writeRateRampUpMinutes,
              writeRateRampUpSteps);
      if (workerRateLimiter == null) {
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

    @Teardown
    public void teardown() {
      try {
        if (bundleState != null) {
          bundleState.awaitQuiescenceQuietly();
          bundleState = null;
        }
      } finally {
        if (executor != null) {
          executor.shutdown();
        }
        MongoDbTransforms.closeIfNotShared(mongoClient);
      }
    }

    @StartBundle
    public void startBundle() {
      if (bundleState != null) {
        bundleState.awaitQuiescenceQuietly();
        bundleState = null;
      }
      if (lastDlqLogTimeMs == null) {
        lastDlqLogTimeMs = new AtomicLong(0L);
      }
      if (mongoClient == null) {
        mongoClient = clientFactory.apply(uri);
      }
      bundleState = new BundleWriteState();
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
      while ((failure = bundleState.failures.poll()) != null) {
        c.output(failureTag, failure);
      }
    }

    private void flushBatch() throws InterruptedException {
      if (currentBatch.isEmpty()) {
        return;
      }
      final BundleWriteState activeBundle = bundleState;
      List<DocumentWithMetadata> items = currentBatch;
      currentBatch = new ArrayList<>();

      Map<String, List<WriteModel<Document>>> updatesByCollection = new HashMap<>();
      Map<String, List<DocumentWithMetadata>> itemsByCollection = new HashMap<>();

      for (DocumentWithMetadata item : items) {
        String targetCol = item.getTargetCollection();
        if (targetCol == null || targetCol.isEmpty()) {
          targetCol = item.getSourceCollection();
        }
        if (targetCol == null || targetCol.isEmpty()) {
          LOG.warn("Received document without target collection name; routing to DLQ.");
          writePermanentDlqMessage(
              activeBundle,
              Collections.singletonList(item),
              "Received document without target collection name");
          continue;
        }

        if (item.getOperationType() != null && item.getOperationType().isDelete()) {
          if (item.hasId()) {
            Object id = item.getId();
            updatesByCollection
                .computeIfAbsent(targetCol, k -> new ArrayList<>())
                .add(new DeleteOneModel<>(new Document("_id", id)));
            itemsByCollection.computeIfAbsent(targetCol, k -> new ArrayList<>()).add(item);
            writeDeletes.inc();
          } else {
            LOG.warn("Received DELETE event with null ID; routing to DLQ.");
            writePermanentDlqMessage(
                activeBundle,
                Collections.singletonList(item),
                "Received DELETE event with null ID");
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
            if (doc.containsKey("_id")) {
              Object id = doc.get("_id");
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
                  activeBundle,
                  Collections.singletonList(item),
                  "Received document without '_id' field");
            }
          } else {
            LOG.warn(
                "Received upsert event with null document payload for key '{}'; routing to DLQ.",
                item.getDedupKey());
            writePermanentDlqMessage(
                activeBundle,
                Collections.singletonList(item),
                "Received upsert event with null document payload");
          }
        }
      }

      if (!updatesByCollection.isEmpty()) {
        batchesFlushed.inc();
        if (workerRateLimiter != null && !items.isEmpty()) {
          workerRateLimiter.acquire(items.size());
        }
        LOG.debug(
            "Flushing batch of {} documents across {} target collection(s) to MongoDB (active"
                + " async write futures in queue: {})",
            items.size(),
            updatesByCollection.size(),
            activeBundle.futures.size());
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

                      MongoCollection<Document> col;
                      try {
                        col = mongoClient.getDatabase(database).getCollection(colName);
                      } catch (IllegalArgumentException iae) {
                        LOG.warn(
                            "Invalid target collection '{}' in database '{}'; routing {} items to DLQ.",
                            colName,
                            database,
                            currentItemList.size(),
                            iae);
                        writePermanentDlqMessage(
                            activeBundle,
                            currentItemList,
                            "Invalid target collection name '"
                                + colName
                                + "': "
                                + iae.getMessage());
                        continue;
                      }

                      writeBatchWithRetry(
                          activeBundle, colName, col, currentUpdates, currentItemList);
                    }
                  } finally {
                    semaphore.release();
                  }
                },
                executor);
        activeBundle.futures.add(future);
      }
    }

    private void writeBatchWithRetry(
        BundleWriteState activeBundle,
        String colName,
        MongoCollection<Document> col,
        List<WriteModel<Document>> currentUpdates,
        List<DocumentWithMetadata> currentItemList) {
      BackOff backoff = backoffSpec.backoff();
      Sleeper sleeper = Sleeper.DEFAULT;

      while (true) {
        try {
          col.bulkWrite(currentUpdates, new BulkWriteOptions().ordered(false));
          activeBundle.successfulCount.addAndGet(currentItemList.size());
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
            activeBundle.incDynamicCounter(
                "inMemoryRetries",
                "MongoBulkWriteException_WriteConcern",
                wcError.getCode(),
                currentItemList.size());
            activeBundle.inMemoryRetriesCount.addAndGet(currentItemList.size());
            if (handleBackoff(activeBundle, sleeper, backoff, currentItemList)) {
              break;
            }
            continue;
          }

          if (wcError == null) {
            activeBundle.successfulCount.addAndGet(currentItemList.size() - writeErrors.size());
          }
          LOG.warn(
              "Transient MongoBulkWriteException on collection '{}' (errors={}, wcError={})."
                  + " Evaluating retry batch after backoff",
              colName,
              writeErrors.size(),
              wcError != null ? wcError.getCode() : "none");

          List<WriteModel<Document>> nextUpdates = new ArrayList<>();
          List<DocumentWithMetadata> nextItemList = new ArrayList<>();
          generateRetryBatch(
              activeBundle,
              writeErrors,
              wcError,
              currentUpdates,
              currentItemList,
              nextUpdates,
              nextItemList);

          if (nextUpdates.isEmpty()) {
            break;
          }

          if (handleBackoff(activeBundle, sleeper, backoff, nextItemList)) {
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
            activeBundle.incDynamicCounter(
                "severeFailedWrites", e.getClass().getSimpleName(), code, currentItemList.size());
            activeBundle.severeFailedWritesCount.addAndGet(currentItemList.size());
            LOG.error(
                "Permanent write failure on collection '{}' (code={}): {}",
                colName,
                code,
                e.getMessage());
            writePermanentDlqMessage(
                activeBundle, currentItemList, "Failed to write documents: " + e.getMessage());
            break;
          }

          activeBundle.incDynamicCounter(
              "inMemoryRetries", e.getClass().getSimpleName(), code, currentItemList.size());
          activeBundle.inMemoryRetriesCount.addAndGet(currentItemList.size());
          LOG.warn(
              "Transient write exception on collection '{}': {}. Retrying {} documents after"
                  + " backoff",
              colName,
              e.getMessage(),
              currentItemList.size());
          if (handleBackoff(activeBundle, sleeper, backoff, currentItemList)) {
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
        BundleWriteState activeBundle,
        List<BulkWriteError> writeErrors,
        WriteConcernError wcError,
        List<WriteModel<Document>> currentUpdates,
        List<DocumentWithMetadata> currentItemList,
        List<WriteModel<Document>> nextUpdates,
        List<DocumentWithMetadata> nextItemList) {
      boolean[] seenWriteError = wcError != null ? new boolean[currentItemList.size()] : null;
      for (BulkWriteError error : writeErrors) {
        int index = error.getIndex();
        if (index >= 0 && index < currentItemList.size()) {
          if (seenWriteError != null) {
            seenWriteError[index] = true;
          }
          DocumentWithMetadata failedItem = currentItemList.get(index);
          WriteModel<Document> failedUpdate = currentUpdates.get(index);

          if (isPermanentErrorCode(error.getCode())) {
            activeBundle.incDynamicCounter(
                "severeFailedWrites", "MongoBulkWriteException", error.getCode(), 1);
            activeBundle.severeFailedWritesCount.addAndGet(1);
            writePermanentDlqMessage(
                activeBundle,
                Collections.singletonList(failedItem),
                "Permanent failure writing document. Error: " + error.getMessage());
          } else {
            activeBundle.incDynamicCounter(
                "inMemoryRetries", "MongoBulkWriteException", error.getCode(), 1);
            activeBundle.inMemoryRetriesCount.addAndGet(1);
            nextUpdates.add(failedUpdate);
            nextItemList.add(failedItem);
          }
        }
      }
      if (wcError != null && seenWriteError != null) {
        int wcRetryCount = 0;
        for (int i = 0; i < currentItemList.size(); i++) {
          if (!seenWriteError[i]) {
            nextUpdates.add(currentUpdates.get(i));
            nextItemList.add(currentItemList.get(i));
            wcRetryCount++;
          }
        }
        if (wcRetryCount > 0) {
          activeBundle.incDynamicCounter(
              "inMemoryRetries",
              "MongoBulkWriteException_WriteConcern",
              wcError.getCode(),
              wcRetryCount);
          activeBundle.inMemoryRetriesCount.addAndGet(wcRetryCount);
        }
      }
    }

    private void writePermanentDlqMessage(
        BundleWriteState activeBundle, List<DocumentWithMetadata> itemList, String message) {
      writeToDlq(activeBundle, itemList, message, true);
    }

    private void writeRetryableDlqMessage(
        BundleWriteState activeBundle, List<DocumentWithMetadata> itemList, String message) {
      writeToDlq(activeBundle, itemList, message, false);
    }

    private void writeToDlq(
        BundleWriteState activeBundle,
        List<DocumentWithMetadata> itemList,
        String message,
        boolean isPermanent) {
      long now = System.currentTimeMillis();
      long prev = lastDlqLogTimeMs != null ? lastDlqLogTimeMs.get() : 0L;
      if ((prev == 0L || now - prev >= DLQ_LOG_INTERVAL_MS)
          && (lastDlqLogTimeMs == null || lastDlqLogTimeMs.compareAndSet(prev, now))) {
        String sampleId = !itemList.isEmpty() ? String.valueOf(itemList.get(0).getId()) : "N/A";
        LOG.warn(
            "DLQ Error Summary (logged at most once every 30s per worker thread): {} document(s)"
                + " sent to DLQ in this batch. Reason: {} [Sample Doc ID: {}]",
            itemList.size(),
            message,
            sampleId);
      }
      long permCount = 0;
      long retryDlqCount = 0;
      for (DocumentWithMetadata item : itemList) {
        int nextRetryCount = (item.getRetryCount() != null ? item.getRetryCount() : 0) + 1;
        boolean isPerm = isPermanent || (dlqMaxRetries != null && nextRetryCount > dlqMaxRetries);
        int retryCount =
            isPerm
                ? Math.max(nextRetryCount, dlqMaxRetries != null ? dlqMaxRetries + 1 : 1)
                : nextRetryCount;
        ErrorType errorType = isPerm ? PERMANENT : RETRYABLE;
        if (isPerm) {
          permCount++;
        } else {
          retryDlqCount++;
        }

        activeBundle.failures.add(
            item.withFailure(message, errorType, FailureStage.WRITE, retryCount));
      }
      if (permCount > 0) {
        activeBundle.permanentFailuresCount.addAndGet(permCount);
      }
      if (retryDlqCount > 0) {
        activeBundle.dlqRetriesCount.addAndGet(retryDlqCount);
      }
    }

    private boolean handleBackoff(
        BundleWriteState activeBundle,
        Sleeper sleeper,
        BackOff backoff,
        List<DocumentWithMetadata> itemList) {
      try {
        if (!BackOffUtils.next(sleeper, backoff)) {
          writeRetryableDlqMessage(
              activeBundle, itemList, "Backoff exhausted. Failed to write documents");
          return true;
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        writeRetryableDlqMessage(activeBundle, itemList, "Interrupted while writing documents");
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

      BundleWriteState state = bundleState;
      if (state == null) {
        return;
      }
      try {
        state.awaitQuiescence();
      } finally {
        bundleState = null;
      }

      successfulWrites.inc(state.successfulCount.get());
      inMemoryRetries.inc(state.inMemoryRetriesCount.get());
      severeFailedWrites.inc(state.severeFailedWritesCount.get());
      dlqRetries.inc(state.dlqRetriesCount.get());
      permanentFailures.inc(state.permanentFailuresCount.get());
      state.dynamicCounters.forEach(
          (name, count) -> Metrics.counter(WriteWithDlq.class, name).inc(count.get()));

      DocumentWithMetadata failure;
      while ((failure = state.failures.poll()) != null) {
        c.output(failureTag, failure, Instant.now(), GlobalWindow.INSTANCE);
      }

      long succ = state.successfulCount.get();
      long memRetries = state.inMemoryRetriesCount.get();
      long dlqRet = state.dlqRetriesCount.get();
      long permFail = state.permanentFailuresCount.get();
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
    private final Integer maxConcurrentAsyncWrites;
    private final Integer maxWriteRetries;
    private final Integer dlqMaxRetries;
    private final Integer initialWriteRatePerWorker;
    private final Integer writeRateRampUpMinutes;
    private final Integer writeRateRampUpSteps;
    private final Integer maxWriteRatePerWorker;
    private final SerializableFunction<String, MongoClient> clientFactory;
    private final TupleTag<DocumentWithMetadata> failureTag;
    private final String rateLimiterKey = UUID.randomUUID().toString();
    private transient FluentBackoff backoffSpec;
    private transient WorkerRateLimiter workerRateLimiter;

    private final Counter successfulWrites =
        Metrics.counter(WriteWithDlq.class, "successfulWrites");
    private final Counter inMemoryRetries = Metrics.counter(WriteWithDlq.class, "inMemoryRetries");
    private final Counter severeFailedWrites =
        Metrics.counter(WriteWithDlq.class, "severeFailedWrites");
    private final Counter dlqRetries = Metrics.counter(WriteWithDlq.class, "dlqRetries");
    private final Counter permanentFailures =
        Metrics.counter(WriteWithDlq.class, "permanentFailures");
    private final Counter batchesFlushed = Metrics.counter(WriteWithDlq.class, "batchesFlushed");
    private final Counter writeInsertsUpserts =
        Metrics.counter(WriteWithDlq.class, "writeInsertsUpserts");
    private final Counter writeDeletes = Metrics.counter(WriteWithDlq.class, "writeDeletes");
    private final Counter writeDropsSkipped =
        Metrics.counter(WriteWithDlq.class, "writeDropsSkipped");
    private final Counter writeBatchesCoalesced =
        Metrics.counter(WriteWithDlq.class, "writeBatchesCoalesced");
    private final Distribution bulkWriteLatencyMs =
        Metrics.distribution(WriteWithDlq.class, "bulkWriteLatencyMs");

    private transient MongoClient mongoClient;
    private transient SharedWritePool writePool;
    private transient BundleWriteState bundleState;
    private transient Map<String, CompletableFuture<Void>> inFlightByKey;
    private transient AtomicLong lastDlqLogTimeMs;

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
    WorkerRateLimiter getRateLimiter() {
      return (workerRateLimiter != null && !workerRateLimiter.isDisabled())
          ? workerRateLimiter
          : null;
    }

    @VisibleForTesting
    void setStartTimeMs(long startTimeMs) {
      if (workerRateLimiter != null) {
        workerRateLimiter.setStartTimeMs(startTimeMs);
      }
    }

    @VisibleForTesting
    void updateRateLimiterForTest() {
      if (workerRateLimiter != null) {
        workerRateLimiter.updateRate(System.currentTimeMillis());
      }
    }

    @Setup
    public void setup() {
      writePool = getOrCreateWritePool(maxConcurrentAsyncWrites);
      lastDlqLogTimeMs = new AtomicLong(0L);
      if (clientFactory != null && uri != null) {
        mongoClient = clientFactory.apply(uri);
      }
      backoffSpec =
          FluentBackoff.DEFAULT
              .withMaxRetries(maxWriteRetries)
              .withInitialBackoff(Duration.millis(200))
              .withExponent(2.0);
      workerRateLimiter =
          getOrCreateWorkerRateLimiter(
              rateLimiterKey,
              initialWriteRatePerWorker,
              maxWriteRatePerWorker,
              writeRateRampUpMinutes,
              writeRateRampUpSteps);
      if (workerRateLimiter == null) {
        LOG.info(
            "Write rate limiting is disabled for WriteBatchesFn (initialWriteRatePerWorker <= 0)");
      }
      LOG.info(
          "Initialized MongoDB WriteBatchesFn worker thread for database '{}'"
              + " (maxConcurrentAsyncWrites={}, maxWriteRetries={})",
          database,
          maxConcurrentAsyncWrites,
          maxWriteRetries);
    }

    @Teardown
    public void teardown() {
      try {
        if (bundleState != null) {
          bundleState.awaitQuiescenceQuietly();
          bundleState = null;
        }
      } finally {
        if (inFlightByKey != null) {
          inFlightByKey.clear();
        }
        MongoDbTransforms.closeIfNotShared(mongoClient);
      }
    }

    @StartBundle
    public void startBundle() {
      if (bundleState != null) {
        bundleState.awaitQuiescenceQuietly();
        bundleState = null;
      }
      if (lastDlqLogTimeMs == null) {
        lastDlqLogTimeMs = new AtomicLong(0L);
      }
      if (writePool == null) {
        writePool = getOrCreateWritePool(maxConcurrentAsyncWrites);
      }
      if (mongoClient == null && clientFactory != null && uri != null) {
        mongoClient = clientFactory.apply(uri);
      }
      bundleState = new BundleWriteState();
      inFlightByKey = new HashMap<>();
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
      while ((failure = bundleState.failures.poll()) != null) {
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

    private static final class NumericCoalesceKey {
      private final String canonicalKey;

      NumericCoalesceKey(String canonicalKey) {
        this.canonicalKey = canonicalKey;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (!(o instanceof NumericCoalesceKey that)) {
          return false;
        }
        return Objects.equals(canonicalKey, that.canonicalKey);
      }

      @Override
      public int hashCode() {
        return Objects.hash(NumericCoalesceKey.class, canonicalKey);
      }

      @Override
      public String toString() {
        return canonicalKey;
      }
    }

    private static boolean requiresCanonicalCoalesceKey(Object id) {
      return id == null
          || id instanceof Map
          || id instanceof byte[]
          || id instanceof Binary
          || id instanceof BsonValue
          || id instanceof Symbol
          || id instanceof Pattern
          || id instanceof Iterable
          || id instanceof CodeWithScope;
    }

    private static Object canonicalCoalesceKey(Object id, DocumentWithMetadata item) {
      String numericKey = DocumentWithMetadata.canonicalNumericIdKey(id);
      if (numericKey != null) {
        return new NumericCoalesceKey(numericKey);
      }
      if (requiresCanonicalCoalesceKey(id)) {
        return item.getDedupKey() != null ? item.getDedupKey() : id;
      }
      return id;
    }

    private void putCoalescedOp(
        Map<String, LinkedHashMap<Object, CoalescedOp>> coalescedByCollection,
        String targetCol,
        Object id,
        WriteModel<Document> model,
        DocumentWithMetadata item,
        boolean isDelete) {
      LinkedHashMap<Object, CoalescedOp> colMap =
          coalescedByCollection.computeIfAbsent(targetCol, k -> new LinkedHashMap<>());
      // Normalize null, composite/binary/regex/CodeWithScope _id types via canonical Extended JSON
      // dedupKey and numeric _id types via canonical mathematical value to match MongoDB's _id
      // index equality semantics.
      Object coalesceKey = canonicalCoalesceKey(id, item);
      CoalescedOp prev = colMap.get(coalesceKey);
      if (prev != null) {
        writeBatchesCoalesced.inc();
        if (item.getTimestampSortKey() != null
            && prev.item.getTimestampSortKey() != null
            && item.getTimestampSortKey().compareTo(prev.item.getTimestampSortKey()) < 0) {
          return;
        }
      }
      colMap.put(coalesceKey, new CoalescedOp(model, item, isDelete));
    }

    private void flushBatch(List<DocumentWithMetadata> items) throws InterruptedException {
      if (items == null || items.isEmpty()) {
        return;
      }
      final BundleWriteState activeBundle = bundleState;

      Map<String, LinkedHashMap<Object, CoalescedOp>> coalescedByCollection = new HashMap<>();

      for (DocumentWithMetadata item : items) {
        String targetCol = item.getTargetCollection();
        if (targetCol == null || targetCol.isEmpty()) {
          targetCol = item.getSourceCollection();
        }
        if (targetCol == null || targetCol.isEmpty()) {
          LOG.warn("Received document without target collection name; routing to DLQ.");
          writePermanentDlqMessage(
              activeBundle,
              Collections.singletonList(item),
              "Received document without target collection name");
          continue;
        }

        if (item.getOperationType() != null && item.getOperationType().isDelete()) {
          if (item.hasId()) {
            Object id = item.getId();
            putCoalescedOp(
                coalescedByCollection,
                targetCol,
                id,
                new DeleteOneModel<>(new Document("_id", id)),
                item,
                /* isDelete= */ true);
          } else {
            LOG.warn("Received DELETE event with null ID; routing to DLQ.");
            writePermanentDlqMessage(
                activeBundle,
                Collections.singletonList(item),
                "Received DELETE event with null ID");
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
            if (doc.containsKey("_id")) {
              Object id = doc.get("_id");
              putCoalescedOp(
                  coalescedByCollection,
                  targetCol,
                  id,
                  new ReplaceOneModel<>(
                      new Document("_id", id), doc, new ReplaceOptions().upsert(true)),
                  item,
                  /* isDelete= */ false);
            } else {
              LOG.warn("Received document without '_id' field; routing to DLQ.");
              writePermanentDlqMessage(
                  activeBundle,
                  Collections.singletonList(item),
                  "Received document without '_id' field");
            }
          } else {
            LOG.warn(
                "Received upsert event with null document payload for key '{}'; routing to DLQ.",
                item.getDedupKey());
            writePermanentDlqMessage(
                activeBundle,
                Collections.singletonList(item),
                "Received upsert event with null document payload");
          }
        }
      }

      if (!coalescedByCollection.isEmpty()) {
        batchesFlushed.inc();

        int totalCoalescedCount = 0;
        for (LinkedHashMap<Object, CoalescedOp> colMap : coalescedByCollection.values()) {
          totalCoalescedCount += colMap.size();
        }

        if (workerRateLimiter != null && totalCoalescedCount > 0) {
          workerRateLimiter.acquire(totalCoalescedCount);
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

          MongoCollection<Document> col;
          try {
            col = mongoClient.getDatabase(database).getCollection(colName);
          } catch (IllegalArgumentException iae) {
            LOG.warn(
                "Invalid target collection '{}' in database '{}'; routing {} items to DLQ.",
                colName,
                database,
                currentItemList.size(),
                iae);
            writePermanentDlqMessage(
                activeBundle,
                currentItemList,
                "Invalid target collection name '" + colName + "': " + iae.getMessage());
            continue;
          }

          List<CompletableFuture<Void>> priorFutures = null;
          if (inFlightByKey != null) {
            for (Object coalesceKey : colMap.keySet()) {
              String scopedKey = colName + "#" + coalesceKey;
              CompletableFuture<Void> prior = inFlightByKey.get(scopedKey);
              if (prior != null && !prior.isDone()) {
                if (priorFutures == null) {
                  priorFutures = new ArrayList<>(4);
                }
                priorFutures.add(prior);
              }
            }
          }
          if (priorFutures != null) {
            for (CompletableFuture<Void> prior : priorFutures) {
              try {
                prior.join();
              } catch (Throwable ignored) {
                // Only serialize write ordering after the prior in-flight RPC completes.
              }
            }
          }

          writePool.semaphore.acquire();
          CompletableFuture<Void> future =
              CompletableFuture.runAsync(
                  () -> {
                    try {
                      writeBatchWithRetry(
                          activeBundle, colName, col, currentUpdates, currentItemList);
                    } finally {
                      writePool.semaphore.release();
                    }
                  },
                  writePool.executor);
          activeBundle.futures.add(future);
          if (inFlightByKey != null) {
            for (Object coalesceKey : colMap.keySet()) {
              inFlightByKey.put(colName + "#" + coalesceKey, future);
            }
          }
        }
      }
    }

    private void writeBatchWithRetry(
        BundleWriteState activeBundle,
        String colName,
        MongoCollection<Document> col,
        List<WriteModel<Document>> currentUpdates,
        List<DocumentWithMetadata> currentItemList) {
      BackOff backoff = backoffSpec.backoff();
      Sleeper sleeper = Sleeper.DEFAULT;

      while (true) {
        long writeStartMs = System.currentTimeMillis();
        try {
          col.bulkWrite(currentUpdates, new BulkWriteOptions().ordered(false));
          long writeLatency = System.currentTimeMillis() - writeStartMs;
          activeBundle.writeLatenciesMs.add(writeLatency);
          activeBundle.successfulCount.addAndGet(currentItemList.size());
          LOG.debug(
              "Successfully bulk-wrote {} documents to collection '{}' in {}ms",
              currentItemList.size(),
              colName,
              writeLatency);
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
            activeBundle.incDynamicCounter(
                "inMemoryRetries",
                "MongoBulkWriteException_WriteConcern",
                wcError.getCode(),
                currentItemList.size());
            activeBundle.inMemoryRetriesCount.addAndGet(currentItemList.size());
            if (handleBackoff(activeBundle, sleeper, backoff, currentItemList)) {
              break;
            }
            continue;
          }

          if (wcError == null) {
            activeBundle.successfulCount.addAndGet(currentItemList.size() - writeErrors.size());
          }
          LOG.warn(
              "Transient MongoBulkWriteException on collection '{}' (errors={}, wcError={})."
                  + " Evaluating retry batch after backoff",
              colName,
              writeErrors.size(),
              wcError != null ? wcError.getCode() : "none");

          List<WriteModel<Document>> nextUpdates = new ArrayList<>();
          List<DocumentWithMetadata> nextItemList = new ArrayList<>();
          generateRetryBatch(
              activeBundle,
              writeErrors,
              wcError,
              currentUpdates,
              currentItemList,
              nextUpdates,
              nextItemList);

          if (nextUpdates.isEmpty()) {
            break;
          }

          if (handleBackoff(activeBundle, sleeper, backoff, nextItemList)) {
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
            activeBundle.incDynamicCounter(
                "severeFailedWrites", e.getClass().getSimpleName(), code, currentItemList.size());
            activeBundle.severeFailedWritesCount.addAndGet(currentItemList.size());
            LOG.error(
                "Permanent write failure on collection '{}' (code={}): {}",
                colName,
                code,
                e.getMessage());
            writePermanentDlqMessage(
                activeBundle, currentItemList, "Failed to write documents: " + e.getMessage());
            break;
          }

          activeBundle.incDynamicCounter(
              "inMemoryRetries", e.getClass().getSimpleName(), code, currentItemList.size());
          activeBundle.inMemoryRetriesCount.addAndGet(currentItemList.size());
          LOG.warn(
              "Transient write exception on collection '{}': {}. Retrying {} documents after"
                  + " backoff",
              colName,
              e.getMessage(),
              currentItemList.size());
          if (handleBackoff(activeBundle, sleeper, backoff, currentItemList)) {
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
        BundleWriteState activeBundle,
        List<BulkWriteError> writeErrors,
        WriteConcernError wcError,
        List<WriteModel<Document>> currentUpdates,
        List<DocumentWithMetadata> currentItemList,
        List<WriteModel<Document>> nextUpdates,
        List<DocumentWithMetadata> nextItemList) {
      boolean[] seenWriteError = wcError != null ? new boolean[currentItemList.size()] : null;
      for (BulkWriteError error : writeErrors) {
        int index = error.getIndex();
        if (index >= 0 && index < currentItemList.size()) {
          if (seenWriteError != null) {
            seenWriteError[index] = true;
          }
          DocumentWithMetadata failedItem = currentItemList.get(index);
          WriteModel<Document> failedUpdate = currentUpdates.get(index);

          if (isPermanentErrorCode(error.getCode())) {
            activeBundle.incDynamicCounter(
                "severeFailedWrites", "MongoBulkWriteException", error.getCode(), 1);
            activeBundle.severeFailedWritesCount.addAndGet(1);
            writePermanentDlqMessage(
                activeBundle,
                Collections.singletonList(failedItem),
                "Permanent failure writing document. Error: " + error.getMessage());
          } else {
            activeBundle.incDynamicCounter(
                "inMemoryRetries", "MongoBulkWriteException", error.getCode(), 1);
            activeBundle.inMemoryRetriesCount.addAndGet(1);
            nextUpdates.add(failedUpdate);
            nextItemList.add(failedItem);
          }
        }
      }
      if (wcError != null && seenWriteError != null) {
        int wcRetryCount = 0;
        for (int i = 0; i < currentItemList.size(); i++) {
          if (!seenWriteError[i]) {
            nextUpdates.add(currentUpdates.get(i));
            nextItemList.add(currentItemList.get(i));
            wcRetryCount++;
          }
        }
        if (wcRetryCount > 0) {
          activeBundle.incDynamicCounter(
              "inMemoryRetries",
              "MongoBulkWriteException_WriteConcern",
              wcError.getCode(),
              wcRetryCount);
          activeBundle.inMemoryRetriesCount.addAndGet(wcRetryCount);
        }
      }
    }

    private void writePermanentDlqMessage(
        BundleWriteState activeBundle, List<DocumentWithMetadata> itemList, String message) {
      writeToDlq(activeBundle, itemList, message, true);
    }

    private void writeRetryableDlqMessage(
        BundleWriteState activeBundle, List<DocumentWithMetadata> itemList, String message) {
      writeToDlq(activeBundle, itemList, message, false);
    }

    private void writeToDlq(
        BundleWriteState activeBundle,
        List<DocumentWithMetadata> itemList,
        String message,
        boolean isPermanent) {
      long now = System.currentTimeMillis();
      long prev = lastDlqLogTimeMs != null ? lastDlqLogTimeMs.get() : 0L;
      if ((prev == 0L || now - prev >= DLQ_LOG_INTERVAL_MS)
          && (lastDlqLogTimeMs == null || lastDlqLogTimeMs.compareAndSet(prev, now))) {
        String sampleId = !itemList.isEmpty() ? String.valueOf(itemList.get(0).getId()) : "N/A";
        LOG.warn(
            "DLQ Error Summary (logged at most once every 30s per worker thread): {} document(s)"
                + " sent to DLQ in this batch. Reason: {} [Sample Doc ID: {}]",
            itemList.size(),
            message,
            sampleId);
      }
      long permCount = 0;
      long retryDlqCount = 0;
      for (DocumentWithMetadata item : itemList) {
        int nextRetryCount = (item.getRetryCount() != null ? item.getRetryCount() : 0) + 1;
        boolean isPerm = isPermanent || (dlqMaxRetries != null && nextRetryCount > dlqMaxRetries);
        int retryCount =
            isPerm
                ? Math.max(nextRetryCount, dlqMaxRetries != null ? dlqMaxRetries + 1 : 1)
                : nextRetryCount;
        ErrorType errorType = isPerm ? PERMANENT : RETRYABLE;
        if (isPerm) {
          permCount++;
        } else {
          retryDlqCount++;
        }

        activeBundle.failures.add(
            item.withFailure(message, errorType, FailureStage.WRITE, retryCount));
      }
      if (permCount > 0) {
        activeBundle.permanentFailuresCount.addAndGet(permCount);
      }
      if (retryDlqCount > 0) {
        activeBundle.dlqRetriesCount.addAndGet(retryDlqCount);
      }
    }

    private boolean handleBackoff(
        BundleWriteState activeBundle,
        Sleeper sleeper,
        BackOff backoff,
        List<DocumentWithMetadata> itemList) {
      try {
        if (!BackOffUtils.next(sleeper, backoff)) {
          writeRetryableDlqMessage(
              activeBundle, itemList, "Backoff exhausted. Failed to write documents");
          return true;
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        writeRetryableDlqMessage(activeBundle, itemList, "Interrupted while writing documents");
        return true;
      }
      return false;
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      BundleWriteState state = bundleState;
      if (state == null) {
        return;
      }
      try {
        state.awaitQuiescence();
      } finally {
        bundleState = null;
        if (inFlightByKey != null) {
          inFlightByKey.clear();
        }
      }
      successfulWrites.inc(state.successfulCount.get());
      inMemoryRetries.inc(state.inMemoryRetriesCount.get());
      severeFailedWrites.inc(state.severeFailedWritesCount.get());
      dlqRetries.inc(state.dlqRetriesCount.get());
      permanentFailures.inc(state.permanentFailuresCount.get());
      state.dynamicCounters.forEach(
          (name, count) -> Metrics.counter(WriteWithDlq.class, name).inc(count.get()));
      Long latencyMs;
      while ((latencyMs = state.writeLatenciesMs.poll()) != null) {
        bulkWriteLatencyMs.update(latencyMs);
      }

      DocumentWithMetadata failure;
      while ((failure = state.failures.poll()) != null) {
        c.output(failureTag, failure, Instant.now(), GlobalWindow.INSTANCE);
      }

      long succ = state.successfulCount.get();
      long memRetries = state.inMemoryRetriesCount.get();
      long dlqRet = state.dlqRetriesCount.get();
      long permFail = state.permanentFailuresCount.get();
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

    private final String fileSystemPath;
    private final String functionName;
    private final Integer reloadIntervalMinutes;
    private final Integer dlqMaxRetries;
    private final TupleTag<DocumentWithMetadata> failureTag;
    private transient JavascriptRuntime javascriptRuntime;
    private final Counter udfProcessingFailures =
        Metrics.counter(ApplyUdfFn.class, "udfProcessingFailures");

    public ApplyUdfFn(
        String fileSystemPath,
        String functionName,
        Integer reloadIntervalMinutes,
        TupleTag<DocumentWithMetadata> failureTag) {
      this(fileSystemPath, functionName, reloadIntervalMinutes, /* dlqMaxRetries= */ 3, failureTag);
    }

    public ApplyUdfFn(
        String fileSystemPath,
        String functionName,
        Integer reloadIntervalMinutes,
        Integer dlqMaxRetries,
        TupleTag<DocumentWithMetadata> failureTag) {
      this.fileSystemPath = fileSystemPath;
      this.functionName = functionName;
      this.reloadIntervalMinutes = reloadIntervalMinutes;
      this.dlqMaxRetries = dlqMaxRetries;
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

    private DocumentWithMetadata buildUdfFailure(DocumentWithMetadata item, String errorMessage) {
      int nextRetryCount = (item.getRetryCount() != null ? item.getRetryCount() : 0) + 1;
      boolean isPerm = dlqMaxRetries != null && nextRetryCount > dlqMaxRetries;
      int retryCount =
          isPerm
              ? Math.max(nextRetryCount, dlqMaxRetries != null ? dlqMaxRetries + 1 : 1)
              : nextRetryCount;
      ErrorType errorType = isPerm ? PERMANENT : RETRYABLE;
      return item.withFailure(errorMessage, errorType, FailureStage.UDF, retryCount);
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
          if (payload == null) {
            c.output(item);
            return;
          }
          String transformed = javascriptRuntime.invoke(payload);
          if (transformed != null) {
            Document doc = Document.parse(transformed, DocumentWithMetadata.DOCUMENT_CODEC);
            c.output(item.withDocument(doc, payload));
          } else {
            LOG.warn("UDF returned null for document ID: {}", item.getId());
            udfProcessingFailures.inc();
            c.output(failureTag, buildUdfFailure(item, "UDF returned null"));
          }
        } catch (Throwable e) {
          LOG.error("Failed to apply UDF: {}", e.getMessage());
          udfProcessingFailures.inc();
          c.output(failureTag, buildUdfFailure(item, "UDF failed: " + e.getMessage()));
        }
      } else {
        c.output(item);
      }
    }
  }
}
