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

import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoException;
import com.mongodb.MongoSocketException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.GetInitialRestriction;
import org.apache.beam.sdk.transforms.DoFn.GetRestrictionCoder;
import org.apache.beam.sdk.transforms.DoFn.NewTracker;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.Restriction;
import org.apache.beam.sdk.transforms.DoFn.Teardown;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coarse-partitioned Splittable DoFn reader for MongoDB historical backfill.
 *
 * <p>Generates discrete index-slice query partitions per collection using {@link
 * ReadSplitGenerator} and streams micro-batches of documents into Windmill using a Splittable DoFn,
 * eliminating 2GB bundle commit limits and allowing downstream {@link
 * org.apache.beam.sdk.transforms.GroupIntoBatches} to manage bulk writing to target databases.
 */
public class MongoDbBackfillReader {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbBackfillReader.class);
  public static final int DEFAULT_CURSOR_BATCH_SIZE = 2000;
  public static final int DEFAULT_MAX_SPLITS = 10000;
  private static final JsonWriterSettings CANONICAL_JSON_SETTINGS =
      JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

  /** Value object describing a single partitioned backfill query slice. */
  public static class BackfillPartition implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String uri;
    private final String database;
    private final String sourceCollection;
    private final String targetCollection;
    private final String filterJson;
    private final TimestampSortKey timestampSortKey;
    private final int partitionIndex;
    private final int totalPartitions;

    public BackfillPartition(
        String uri,
        String database,
        String sourceCollection,
        String targetCollection,
        String filterJson,
        TimestampSortKey timestampSortKey,
        int partitionIndex,
        int totalPartitions) {
      this.uri = uri;
      this.database = database;
      this.sourceCollection = sourceCollection;
      this.targetCollection = targetCollection;
      this.filterJson = filterJson;
      this.timestampSortKey = timestampSortKey;
      this.partitionIndex = partitionIndex;
      this.totalPartitions = totalPartitions;
    }

    public String getUri() {
      return uri;
    }

    public String getDatabase() {
      return database;
    }

    public String getSourceCollection() {
      return sourceCollection;
    }

    public String getTargetCollection() {
      return targetCollection;
    }

    public String getFilterJson() {
      return filterJson;
    }

    public TimestampSortKey getTimestampSortKey() {
      return timestampSortKey;
    }

    public int getPartitionIndex() {
      return partitionIndex;
    }

    public int getTotalPartitions() {
      return totalPartitions;
    }

    public boolean hasFilter() {
      return filterJson != null && !filterJson.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BackfillPartition that = (BackfillPartition) o;
      return partitionIndex == that.partitionIndex
          && totalPartitions == that.totalPartitions
          && Objects.equals(uri, that.uri)
          && Objects.equals(database, that.database)
          && Objects.equals(sourceCollection, that.sourceCollection)
          && Objects.equals(targetCollection, that.targetCollection)
          && Objects.equals(filterJson, that.filterJson)
          && Objects.equals(timestampSortKey, that.timestampSortKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          uri,
          database,
          sourceCollection,
          targetCollection,
          filterJson,
          timestampSortKey,
          partitionIndex,
          totalPartitions);
    }

    @Override
    public String toString() {
      return "BackfillPartition{"
          + "col='"
          + sourceCollection
          + "', slice="
          + partitionIndex
          + "/"
          + totalPartitions
          + ", filter="
          + (filterJson != null ? filterJson : "[FullScan]")
          + '}';
    }
  }

  /**
   * Value object describing a sequential chain of backfill partitions executed within a single
   * concurrency slot.
   */
  public static class BackfillSlotTask implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int slotId;
    private final int totalSlots;
    private final List<BackfillPartition> partitions;

    public BackfillSlotTask(int slotId, int totalSlots, List<BackfillPartition> partitions) {
      this.slotId = slotId;
      this.totalSlots = totalSlots;
      this.partitions = partitions != null ? partitions : Collections.emptyList();
    }

    public int getSlotId() {
      return slotId;
    }

    public int getTotalSlots() {
      return totalSlots;
    }

    public List<BackfillPartition> getPartitions() {
      return partitions;
    }

    /**
     * Distributes a list of backfill partitions across virtual concurrency slots using
     * round-robin interleaving.
     */
    public static List<BackfillSlotTask> distribute(
        List<BackfillPartition> partitions, int maxConcurrentSlots) {
      if (partitions == null || partitions.isEmpty()) {
        return Collections.emptyList();
      }
      int numSlots = Math.min(Math.max(1, maxConcurrentSlots), partitions.size());
      List<List<BackfillPartition>> slotBuckets = new ArrayList<>(numSlots);
      for (int i = 0; i < numSlots; i++) {
        slotBuckets.add(new ArrayList<>());
      }
      for (int i = 0; i < partitions.size(); i++) {
        slotBuckets.get(i % numSlots).add(partitions.get(i));
      }
      List<BackfillSlotTask> tasks = new ArrayList<>(numSlots);
      for (int i = 0; i < numSlots; i++) {
        tasks.add(new BackfillSlotTask(i, numSlots, slotBuckets.get(i)));
      }
      return tasks;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BackfillSlotTask that = (BackfillSlotTask) o;
      return slotId == that.slotId
          && totalSlots == that.totalSlots
          && Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(slotId, totalSlots, partitions);
    }

    @Override
    public String toString() {
      return "BackfillSlotTask{slot="
          + slotId
          + "/"
          + totalSlots
          + ", partitions="
          + partitions.size()
          + "}";
    }
  }

  /**
   * Generates discrete backfill partitions for a given collection using {@link ReadSplitGenerator}
   * and live type/quantile discovery via a shared MongoClient with adaptive volume sizing.
   */
  public static List<BackfillPartition> generatePartitions(
      MongoClient client,
      String uri,
      String database,
      String sourceCollection,
      String targetCollection,
      int numSplits,
      int targetChunkSize,
      int maxSplits,
      BsonTimestamp t0) {
    TimestampSortKey sortKey = t0 != null ? TimestampSortKey.backfill(t0.getTime()) : null;
    int splits = Math.max(1, numSplits);
    List<BsonDocument> filters =
        ReadSplitGenerator.generateIndexSliceFilters(
            client, database, sourceCollection, splits, targetChunkSize, maxSplits);

    List<BackfillPartition> partitions = new ArrayList<>();
    for (int i = 0; i < filters.size(); i++) {
      BsonDocument filter = filters.get(i);
      String filterJson =
          (filter == null || filter.isEmpty()) ? null : filter.toJson(CANONICAL_JSON_SETTINGS);
      partitions.add(
          new BackfillPartition(
              uri,
              database,
              sourceCollection,
              targetCollection,
              filterJson,
              sortKey,
              i,
              filters.size()));
    }
    return partitions;
  }

  /**
   * Generates discrete backfill partitions for a given collection using {@link ReadSplitGenerator}
   * and live type/quantile discovery via a shared MongoClient with adaptive volume sizing.
   */
  public static List<BackfillPartition> generatePartitions(
      MongoClient client,
      String uri,
      String database,
      String sourceCollection,
      String targetCollection,
      int targetChunkSize,
      int maxSplits,
      BsonTimestamp t0) {
    return generatePartitions(
        client,
        uri,
        database,
        sourceCollection,
        targetCollection,
        1,
        targetChunkSize,
        maxSplits,
        t0);
  }

  /**
   * Generates discrete backfill partitions for a given collection using {@link ReadSplitGenerator}
   * and live type/quantile discovery via a shared MongoClient.
   */
  public static List<BackfillPartition> generatePartitions(
      MongoClient client,
      String uri,
      String database,
      String sourceCollection,
      String targetCollection,
      int numSplits,
      BsonTimestamp t0) {
    return generatePartitions(
        client,
        uri,
        database,
        sourceCollection,
        targetCollection,
        numSplits,
        0,
        Math.max(numSplits, DEFAULT_MAX_SPLITS),
        t0);
  }

  /**
   * Generates discrete backfill partitions for a given collection using algorithmic key-space
   * splitting without requiring a live MongoClient connection with adaptive volume sizing.
   */
  public static List<BackfillPartition> generatePartitions(
      String uri,
      String database,
      String sourceCollection,
      String targetCollection,
      int numSplits,
      int targetChunkSize,
      int maxSplits,
      BsonTimestamp t0) {
    TimestampSortKey sortKey = t0 != null ? TimestampSortKey.backfill(t0.getTime()) : null;
    int effectiveSplits = Math.max(1, Math.min(maxSplits, numSplits));
    List<BsonDocument> filters = ReadSplitGenerator.generateIndexSliceFilters(effectiveSplits);

    List<BackfillPartition> partitions = new ArrayList<>();
    for (int i = 0; i < filters.size(); i++) {
      BsonDocument filter = filters.get(i);
      String filterJson =
          (filter == null || filter.isEmpty()) ? null : filter.toJson(CANONICAL_JSON_SETTINGS);
      partitions.add(
          new BackfillPartition(
              uri,
              database,
              sourceCollection,
              targetCollection,
              filterJson,
              sortKey,
              i,
              filters.size()));
    }
    return partitions;
  }

  /**
   * Generates discrete backfill partitions for a given collection using algorithmic key-space
   * splitting without requiring a live MongoClient connection with adaptive volume sizing.
   */
  public static List<BackfillPartition> generatePartitions(
      String uri,
      String database,
      String sourceCollection,
      String targetCollection,
      int targetChunkSize,
      int maxSplits,
      BsonTimestamp t0) {
    return generatePartitions(
        uri,
        database,
        sourceCollection,
        targetCollection,
        1,
        targetChunkSize,
        maxSplits,
        t0);
  }

  /**
   * Generates discrete backfill partitions for a given collection using algorithmic key-space
   * splitting without requiring a live MongoClient connection.
   */
  public static List<BackfillPartition> generatePartitions(
      String uri,
      String database,
      String sourceCollection,
      String targetCollection,
      int numSplits,
      BsonTimestamp t0) {
    return generatePartitions(
        uri,
        database,
        sourceCollection,
        targetCollection,
        numSplits,
        0,
        Math.max(numSplits, DEFAULT_MAX_SPLITS),
        t0);
  }

  /**
   * Convenience overload generating a single root partition for a collection.
   */
  public static List<BackfillPartition> generatePartitions(
      String uri,
      String database,
      String sourceCollection,
      String targetCollection,
      BsonTimestamp t0) {
    return generatePartitions(uri, database, sourceCollection, targetCollection, 1, t0);
  }

  /**
   * SDF restriction for MongoDB backfill tracking sequential offset, serialized canonical BSON _id,
   * and completion state.
   */
  public static class BackfillRestriction implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long offset;
    private final String lastSeenIdJson;
    private final boolean done;

    public BackfillRestriction() {
      this(0L, null, false);
    }

    public BackfillRestriction(long offset, String lastSeenIdJson, boolean done) {
      this.offset = offset;
      this.lastSeenIdJson = lastSeenIdJson;
      this.done = done;
    }

    public long getOffset() {
      return offset;
    }

    public String getLastSeenIdJson() {
      return lastSeenIdJson;
    }

    public boolean isDone() {
      return done;
    }

    public BsonValue getLastSeenId() {
      if (lastSeenIdJson == null || lastSeenIdJson.isEmpty()) {
        return null;
      }
      try {
        return BsonDocument.parse(lastSeenIdJson).get("_id");
      } catch (Exception e) {
        LOG.warn("Failed parsing lastSeenIdJson: {}", lastSeenIdJson, e);
        return null;
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof BackfillRestriction)) {
        return false;
      }
      BackfillRestriction that = (BackfillRestriction) o;
      return offset == that.offset
          && done == that.done
          && Objects.equals(lastSeenIdJson, that.lastSeenIdJson);
    }

    @Override
    public int hashCode() {
      return Objects.hash(offset, lastSeenIdJson, done);
    }

    @Override
    public String toString() {
      return "BackfillRestriction{offset="
          + offset
          + ", hasLastSeenId="
          + (lastSeenIdJson != null)
          + ", done="
          + done
          + "}";
    }
  }

  /**
   * RestrictionTracker for BackfillRestriction ensuring slice-per-offset claiming,
   * claim-before-output, and checkpoint splitting.
   */
  public static class BackfillRestrictionTracker
      extends RestrictionTracker<BackfillRestriction, BackfillRestriction> {

    private BackfillRestriction currentRestriction;
    private boolean shouldStop = false;

    public BackfillRestrictionTracker(BackfillRestriction restriction) {
      this.currentRestriction =
          restriction != null ? restriction : new BackfillRestriction(0L, null, false);
    }

    @Override
    public boolean tryClaim(BackfillRestriction position) {
      if (shouldStop || (currentRestriction != null && currentRestriction.isDone())) {
        return false;
      }
      this.currentRestriction = position;
      return true;
    }

    @Override
    public BackfillRestriction currentRestriction() {
      return currentRestriction;
    }

    @Override
    public @Nullable SplitResult<BackfillRestriction> trySplit(double fractionOfRemainder) {
      if (fractionOfRemainder == 0) {
        if (shouldStop || currentRestriction.isDone()) {
          return null;
        }
        shouldStop = true;
        BackfillRestriction primary = currentRestriction;
        BackfillRestriction residual =
            new BackfillRestriction(
                currentRestriction.getOffset(),
                currentRestriction.getLastSeenIdJson(),
                currentRestriction.isDone());
        return SplitResult.of(primary, residual);
      }
      return null;
    }

    @Override
    public void checkDone() throws IllegalStateException {
      if (!shouldStop && (currentRestriction == null || !currentRestriction.isDone())) {
        throw new IllegalStateException(
            String.format(
                "Last claimed restriction %s is not marked as done, but execution finished without a split.",
                currentRestriction));
      }
    }

    @Override
    public IsBounded isBounded() {
      return IsBounded.BOUNDED;
    }
  }

  /**
   * Splittable DoFn that executes partitioned MongoDB backfill reads on worker threads, streaming
   * micro-batches of documents into Windmill without exceeding bundle commit limits.
   */
  public static class ProcessBackfillPartitionFn
      extends DoFn<BackfillPartition, DocumentWithMetadata> {

    public static final int MAX_DOCS_PER_SLICE = 2000;
    public static final long MAX_SLICE_DURATION_MS = 10000L;
    public static final long CURSOR_EXPIRATION_TIMEOUT_MS = 300_000L; // 5 minutes idle eviction

    private final SerializableFunction<String, MongoClient> clientFactory;

    private transient ConcurrentHashMap<String, MongoClient> clientCache;
    private transient ConcurrentHashMap<String, PartitionCursorHolder> cursorCache;

    private final Counter backfillDocumentsRead =
        Metrics.counter(MongoDbBackfillReader.class, "backfillDocumentsRead");
    private final Counter backfillSlicesCompleted =
        Metrics.counter(MongoDbBackfillReader.class, "backfillSlicesCompleted");
    private final Counter backfillEmptySlices =
        Metrics.counter(MongoDbBackfillReader.class, "backfillEmptySlices");
    private final Counter backfillReadErrors =
        Metrics.counter(MongoDbBackfillReader.class, "backfillReadErrors");

    /** Holds a cached cursor and tracks its last access timestamp and last seen document ID. */
    public static class PartitionCursorHolder implements AutoCloseable {
      private final MongoCursor<Document> cursor;
      private volatile long lastAccessedMs;
      private volatile String lastSeenIdJson;

      public PartitionCursorHolder(MongoCursor<Document> cursor, String initialLastSeenIdJson) {
        this.cursor = cursor;
        this.lastAccessedMs = System.currentTimeMillis();
        this.lastSeenIdJson = initialLastSeenIdJson;
      }

      public MongoCursor<Document> getCursor() {
        this.lastAccessedMs = System.currentTimeMillis();
        return cursor;
      }

      public String getLastSeenIdJson() {
        return lastSeenIdJson;
      }

      public void setLastSeenIdJson(String lastSeenIdJson) {
        this.lastSeenIdJson = lastSeenIdJson;
        this.lastAccessedMs = System.currentTimeMillis();
      }

      public boolean isExpired(long timeoutMs) {
        return (System.currentTimeMillis() - lastAccessedMs) > timeoutMs;
      }

      @Override
      public void close() {
        if (cursor != null) {
          try {
            cursor.close();
          } catch (Exception ignored) {
          }
        }
      }
    }

    public ProcessBackfillPartitionFn() {
      this(MongoDbTransforms::getOrCreateMongoClient);
    }

    public ProcessBackfillPartitionFn(SerializableFunction<String, MongoClient> clientFactory) {
      this.clientFactory = clientFactory;
    }

    @GetInitialRestriction
    public BackfillRestriction getInitialRestriction(@Element BackfillPartition partition) {
      return new BackfillRestriction(0L, null, false);
    }

    @NewTracker
    public BackfillRestrictionTracker newTracker(
        @Element BackfillPartition partition,
        @Restriction BackfillRestriction restriction) {
      return new BackfillRestrictionTracker(restriction);
    }

    @GetRestrictionCoder
    public Coder<BackfillRestriction> getRestrictionCoder() {
      return SerializableCoder.of(BackfillRestriction.class);
    }

    @ProcessElement
    public ProcessContinuation processElement(
        @Element BackfillPartition partition,
        RestrictionTracker<BackfillRestriction, BackfillRestriction> tracker,
        OutputReceiver<DocumentWithMetadata> receiver) {

      if (cursorCache == null) {
        cursorCache = new ConcurrentHashMap<>();
      }
      if (clientCache == null) {
        clientCache = new ConcurrentHashMap<>();
      }
      evictExpiredCursors();

      BackfillRestriction currentRestriction = tracker.currentRestriction();
      if (currentRestriction.isDone() || !tracker.tryClaim(currentRestriction)) {
        return ProcessContinuation.stop();
      }

      String partitionKey =
          partition.getDatabase()
              + "."
              + partition.getSourceCollection()
              + "#"
              + partition.getPartitionIndex();

      String currentLastSeenIdJson = currentRestriction.getLastSeenIdJson();
      PartitionCursorHolder cursorHolder = cursorCache.get(partitionKey);

      // Validate that cached cursor position matches the incoming restriction
      if (cursorHolder != null) {
        if (!Objects.equals(cursorHolder.getLastSeenIdJson(), currentLastSeenIdJson)) {
          closeCursorForPartition(partitionKey);
          cursorHolder = null;
        }
      }

      if (cursorHolder == null) {
        try {
          MongoClient client =
              clientCache.computeIfAbsent(partition.getUri(), clientFactory::apply);
          MongoDatabase db = client.getDatabase(partition.getDatabase());
          MongoCollection<Document> collection =
              db.getCollection(partition.getSourceCollection());

          Bson baseFilter =
              partition.hasFilter()
                  ? BsonDocument.parse(partition.getFilterJson())
                  : new BsonDocument();

          BsonValue lastSeenId = currentRestriction.getLastSeenId();
          Bson queryFilter;
          if (lastSeenId != null) {
            BsonDocument gtFilter =
                new BsonDocument("_id", new BsonDocument("$gt", lastSeenId));
            if (partition.hasFilter()) {
              queryFilter =
                  new BsonDocument(
                      "$and",
                      new BsonArray(
                          Arrays.asList(
                              BsonDocument.parse(partition.getFilterJson()),
                              gtFilter)));
            } else {
              queryFilter = gtFilter;
            }
          } else {
            queryFilter = baseFilter;
          }

          FindIterable<Document> findIterable =
              collection
                  .find(queryFilter)
                  .sort(new BsonDocument("_id", new BsonInt32(1)))
                  .batchSize(DEFAULT_CURSOR_BATCH_SIZE);

          MongoCursor<Document> cursor = findIterable.iterator();
          cursorHolder = new PartitionCursorHolder(cursor, currentLastSeenIdJson);
          cursorCache.put(partitionKey, cursorHolder);
        } catch (Exception e) {
          backfillReadErrors.inc();
          LOG.warn(
              "Failed opening backfill cursor for partition {}: {}. Retrying in 1s.",
              partition,
              e.getMessage(),
              e);
          closeCursorForPartition(partitionKey);
          return ProcessContinuation.resume().withResumeDelay(Duration.millis(1000));
        }
      }

      long sliceStartTime = System.currentTimeMillis();
      int docsInSlice = 0;
      long currentOffset = currentRestriction.getOffset();
      String lastSeenIdJson = currentLastSeenIdJson;

      try {
        MongoCursor<Document> cursor = cursorHolder.getCursor();
        while (docsInSlice < MAX_DOCS_PER_SLICE
            && (System.currentTimeMillis() - sliceStartTime) < MAX_SLICE_DURATION_MS) {
          if (!cursor.hasNext()) {
            // Reached EOF for this partition
            tracker.tryClaim(
                new BackfillRestriction(currentOffset + docsInSlice, lastSeenIdJson, true));
            closeCursorForPartition(partitionKey);
            backfillSlicesCompleted.inc();
            if (currentOffset + docsInSlice == 0) {
              backfillEmptySlices.inc();
            }
            LOG.info(
                "Completed backfill read for collection '{}' [Slice {}/{}]: total {} documents",
                partition.getSourceCollection(),
                partition.getPartitionIndex(),
                partition.getTotalPartitions(),
                currentOffset + docsInSlice);
            return ProcessContinuation.stop();
          }

          Document doc = cursor.next();
          BsonValue docId = doc.toBsonDocument().get("_id");
          String nextIdJson =
              docId != null
                  ? new BsonDocument("_id", docId).toJson(CANONICAL_JSON_SETTINGS)
                  : null;

          // Claim document position BEFORE emitting (claim-before-output)
          if (!tracker.tryClaim(
              new BackfillRestriction(currentOffset + docsInSlice + 1, nextIdJson, false))) {
            closeCursorForPartition(partitionKey);
            return ProcessContinuation.stop();
          }

          DocumentWithMetadata item =
              partition.getTimestampSortKey() != null
                  ? DocumentWithMetadata.backfillEvent(
                      doc,
                      partition.getSourceCollection(),
                      partition.getTargetCollection(),
                      partition.getTimestampSortKey())
                  : DocumentWithMetadata.of(
                      doc,
                      partition.getSourceCollection(),
                      partition.getTargetCollection());

          receiver.output(item);
          backfillDocumentsRead.inc();
          docsInSlice++;
          lastSeenIdJson = nextIdJson;
          cursorHolder.setLastSeenIdJson(lastSeenIdJson);
        }
      } catch (MongoCursorNotFoundException | MongoSocketException mse) {
        backfillReadErrors.inc();
        LOG.warn(
            "Cached cursor disconnected/expired for partition {}: {}."
                + " Resuming from lastSeenId in 500ms.",
            partition,
            mse.getMessage());
        closeCursorForPartition(partitionKey);
        return ProcessContinuation.resume().withResumeDelay(Duration.millis(500));
      } catch (MongoException me) {
        backfillReadErrors.inc();
        LOG.warn(
            "Transient MongoDB exception reading partition {}: {}. Resuming from lastSeenId in 1s.",
            partition,
            me.getMessage());
        closeCursorForPartition(partitionKey);
        return ProcessContinuation.resume().withResumeDelay(Duration.millis(1000));
      } catch (Exception e) {
        backfillReadErrors.inc();
        LOG.error(
            "Unexpected error reading partition {}: {}. Failing bundle.",
            partition,
            e.getMessage(),
            e);
        closeCursorForPartition(partitionKey);
        throw new RuntimeException("Backfill read failed for partition " + partition, e);
      }

      return ProcessContinuation.resume();
    }

    private void evictExpiredCursors() {
      if (cursorCache != null) {
        cursorCache.entrySet().removeIf(entry -> {
          if (entry.getValue().isExpired(CURSOR_EXPIRATION_TIMEOUT_MS)) {
            entry.getValue().close();
            return true;
          }
          return false;
        });
      }
    }

    private void closeCursorForPartition(String partitionKey) {
      if (cursorCache != null && partitionKey != null) {
        PartitionCursorHolder holder = cursorCache.remove(partitionKey);
        if (holder != null) {
          holder.close();
        }
      }
    }

    @Teardown
    public void teardown() {
      if (cursorCache != null) {
        for (PartitionCursorHolder holder : cursorCache.values()) {
          holder.close();
        }
        cursorCache.clear();
      }
      if (clientCache != null) {
        clientCache.clear();
      }
    }
  }

  /**
   * SDF restriction for slot-based backfill tracking current partition index within the slot,
   * document offset within the active partition, last seen canonical BSON _id, and completion state.
   */
  public static class BackfillSlotRestriction implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int partitionIndexInSlot;
    private final long offsetInCurrentPartition;
    private final String lastSeenIdJson;
    private final boolean done;

    public BackfillSlotRestriction() {
      this(0, 0L, null, false);
    }

    public BackfillSlotRestriction(
        int partitionIndexInSlot,
        long offsetInCurrentPartition,
        String lastSeenIdJson,
        boolean done) {
      this.partitionIndexInSlot = partitionIndexInSlot;
      this.offsetInCurrentPartition = offsetInCurrentPartition;
      this.lastSeenIdJson = lastSeenIdJson;
      this.done = done;
    }

    public int getPartitionIndexInSlot() {
      return partitionIndexInSlot;
    }

    public long getOffsetInCurrentPartition() {
      return offsetInCurrentPartition;
    }

    public String getLastSeenIdJson() {
      return lastSeenIdJson;
    }

    public boolean isDone() {
      return done;
    }

    public BsonValue getLastSeenId() {
      if (lastSeenIdJson == null || lastSeenIdJson.isEmpty()) {
        return null;
      }
      try {
        BsonDocument doc = BsonDocument.parse(lastSeenIdJson);
        return doc.get("_id");
      } catch (Exception e) {
        return null;
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BackfillSlotRestriction that = (BackfillSlotRestriction) o;
      return partitionIndexInSlot == that.partitionIndexInSlot
          && offsetInCurrentPartition == that.offsetInCurrentPartition
          && done == that.done
          && Objects.equals(lastSeenIdJson, that.lastSeenIdJson);
    }

    @Override
    public int hashCode() {
      return Objects.hash(partitionIndexInSlot, offsetInCurrentPartition, lastSeenIdJson, done);
    }

    @Override
    public String toString() {
      return "BackfillSlotRestriction{partIdx="
          + partitionIndexInSlot
          + ", offset="
          + offsetInCurrentPartition
          + ", lastId="
          + (lastSeenIdJson != null ? lastSeenIdJson : "null")
          + ", done="
          + done
          + "}";
    }
  }

  /**
   * SDF restriction tracker for slot-based backfill partitions.
   */
  public static class BackfillSlotRestrictionTracker
      extends RestrictionTracker<BackfillSlotRestriction, BackfillSlotRestriction> {

    private BackfillSlotRestriction currentRestriction;
    private boolean shouldStop = false;

    public BackfillSlotRestrictionTracker(BackfillSlotRestriction initial) {
      this.currentRestriction = initial;
    }

    @Override
    public boolean tryClaim(BackfillSlotRestriction position) {
      if (shouldStop || (currentRestriction != null && currentRestriction.isDone())) {
        return false;
      }
      this.currentRestriction = position;
      return true;
    }

    @Override
    public BackfillSlotRestriction currentRestriction() {
      return currentRestriction;
    }

    @Override
    public @Nullable SplitResult<BackfillSlotRestriction> trySplit(double fractionOfRemainder) {
      if (fractionOfRemainder == 0.0) {
        if (shouldStop || (currentRestriction != null && currentRestriction.isDone())) {
          return null;
        }
        shouldStop = true;
        return SplitResult.of(
            currentRestriction,
            new BackfillSlotRestriction(
                currentRestriction.getPartitionIndexInSlot(),
                currentRestriction.getOffsetInCurrentPartition(),
                currentRestriction.getLastSeenIdJson(),
                currentRestriction.isDone()));
      }
      return null;
    }

    @Override
    public void checkDone() throws IllegalStateException {
      if (!shouldStop && (currentRestriction == null || !currentRestriction.isDone())) {
        throw new IllegalStateException(
            String.format(
                "Last claimed slot restriction %s is not marked as done, but execution finished without a split.",
                currentRestriction));
      }
    }

    @Override
    public IsBounded isBounded() {
      return IsBounded.BOUNDED;
    }
  }

  /**
   * Splittable DoFn executing sequential backfill partitions assigned to a concurrency slot.
   * Caps maximum active cursors against MongoDB to the total number of slots cluster-wide.
   */
  public static class ProcessBackfillSlotFn
      extends DoFn<BackfillSlotTask, DocumentWithMetadata> {

    public static final int MAX_DOCS_PER_SLICE = 2000;
    public static final long MAX_SLICE_DURATION_MS = 10000L;
    public static final long CURSOR_EXPIRATION_TIMEOUT_MS = 300_000L;

    private final SerializableFunction<String, MongoClient> clientFactory;

    private transient ConcurrentHashMap<String, MongoClient> clientCache;
    private transient ConcurrentHashMap<String, ProcessBackfillPartitionFn.PartitionCursorHolder> cursorCache;

    private final Counter backfillDocumentsRead =
        Metrics.counter(MongoDbBackfillReader.class, "backfillDocumentsRead");
    private final Counter backfillSlicesCompleted =
        Metrics.counter(MongoDbBackfillReader.class, "backfillSlicesCompleted");
    private final Counter backfillEmptySlices =
        Metrics.counter(MongoDbBackfillReader.class, "backfillEmptySlices");
    private final Counter backfillReadErrors =
        Metrics.counter(MongoDbBackfillReader.class, "backfillReadErrors");

    public ProcessBackfillSlotFn() {
      this(MongoDbTransforms::getOrCreateMongoClient);
    }

    public ProcessBackfillSlotFn(SerializableFunction<String, MongoClient> clientFactory) {
      this.clientFactory = clientFactory;
    }

    @GetInitialRestriction
    public BackfillSlotRestriction getInitialRestriction(@Element BackfillSlotTask slotTask) {
      return new BackfillSlotRestriction(0, 0L, null, false);
    }

    @NewTracker
    public BackfillSlotRestrictionTracker newTracker(
        @Element BackfillSlotTask slotTask,
        @Restriction BackfillSlotRestriction restriction) {
      return new BackfillSlotRestrictionTracker(restriction);
    }

    @GetRestrictionCoder
    public Coder<BackfillSlotRestriction> getRestrictionCoder() {
      return SerializableCoder.of(BackfillSlotRestriction.class);
    }

    @ProcessElement
    public ProcessContinuation processElement(
        @Element BackfillSlotTask slotTask,
        RestrictionTracker<BackfillSlotRestriction, BackfillSlotRestriction> tracker,
        OutputReceiver<DocumentWithMetadata> receiver) {

      if (cursorCache == null) {
        cursorCache = new ConcurrentHashMap<>();
      }
      if (clientCache == null) {
        clientCache = new ConcurrentHashMap<>();
      }
      evictExpiredCursors();

      BackfillSlotRestriction currentRestriction = tracker.currentRestriction();
      if (currentRestriction.isDone() || !tracker.tryClaim(currentRestriction)) {
        return ProcessContinuation.stop();
      }

      List<BackfillPartition> partitions = slotTask.getPartitions();
      if (partitions.isEmpty()) {
        return ProcessContinuation.stop();
      }

      int partitionIdx = currentRestriction.getPartitionIndexInSlot();
      if (partitionIdx >= partitions.size()) {
        tracker.tryClaim(new BackfillSlotRestriction(partitionIdx, 0L, null, true));
        return ProcessContinuation.stop();
      }

      BackfillPartition partition = partitions.get(partitionIdx);
      String partitionKey =
          partition.getDatabase()
              + "."
              + partition.getSourceCollection()
              + "#"
              + partition.getPartitionIndex();

      String currentLastSeenIdJson = currentRestriction.getLastSeenIdJson();
      ProcessBackfillPartitionFn.PartitionCursorHolder cursorHolder = cursorCache.get(partitionKey);

      if (cursorHolder != null) {
        if (!Objects.equals(cursorHolder.getLastSeenIdJson(), currentLastSeenIdJson)) {
          closeCursorForPartition(partitionKey);
          cursorHolder = null;
        }
      }

      if (cursorHolder == null) {
        try {
          MongoClient client =
              clientCache.computeIfAbsent(partition.getUri(), clientFactory::apply);
          MongoDatabase db = client.getDatabase(partition.getDatabase());
          MongoCollection<Document> collection =
              db.getCollection(partition.getSourceCollection());

          Bson baseFilter =
              partition.hasFilter()
                  ? BsonDocument.parse(partition.getFilterJson())
                  : new BsonDocument();

          BsonValue lastSeenId = currentRestriction.getLastSeenId();
          Bson queryFilter;
          if (lastSeenId != null) {
            BsonDocument gtFilter =
                new BsonDocument("_id", new BsonDocument("$gt", lastSeenId));
            if (partition.hasFilter()) {
              queryFilter =
                  new BsonDocument(
                      "$and",
                      new BsonArray(
                          Arrays.asList(
                              BsonDocument.parse(partition.getFilterJson()),
                              gtFilter)));
            } else {
              queryFilter = gtFilter;
            }
          } else {
            queryFilter = baseFilter;
          }

          FindIterable<Document> findIterable =
              collection
                  .find(queryFilter)
                  .sort(new BsonDocument("_id", new BsonInt32(1)))
                  .batchSize(DEFAULT_CURSOR_BATCH_SIZE);

          MongoCursor<Document> cursor = findIterable.iterator();
          cursorHolder =
              new ProcessBackfillPartitionFn.PartitionCursorHolder(cursor, currentLastSeenIdJson);
          cursorCache.put(partitionKey, cursorHolder);
        } catch (Exception e) {
          backfillReadErrors.inc();
          LOG.warn(
              "Failed opening backfill cursor for partition {} in slot {}: {}. Retrying in 1s.",
              partition,
              slotTask.getSlotId(),
              e.getMessage(),
              e);
          closeCursorForPartition(partitionKey);
          return ProcessContinuation.resume().withResumeDelay(Duration.millis(1000));
        }
      }

      long sliceStartTime = System.currentTimeMillis();
      int docsInSlice = 0;
      long currentOffset = currentRestriction.getOffsetInCurrentPartition();
      String lastSeenIdJson = currentLastSeenIdJson;

      try {
        MongoCursor<Document> cursor = cursorHolder.getCursor();
        while (docsInSlice < MAX_DOCS_PER_SLICE
            && (System.currentTimeMillis() - sliceStartTime) < MAX_SLICE_DURATION_MS) {
          if (!cursor.hasNext()) {
            // Reached EOF for this partition
            closeCursorForPartition(partitionKey);
            backfillSlicesCompleted.inc();
            if (currentOffset + docsInSlice == 0) {
              backfillEmptySlices.inc();
            }
            LOG.info(
                "Completed backfill read for collection '{}' [Slice {}/{} in Slot {}/{}]: total {} documents",
                partition.getSourceCollection(),
                partition.getPartitionIndex(),
                partition.getTotalPartitions(),
                slotTask.getSlotId(),
                slotTask.getTotalSlots(),
                currentOffset + docsInSlice);

            int nextPartitionIdx = partitionIdx + 1;
            if (nextPartitionIdx < partitions.size()) {
              tracker.tryClaim(new BackfillSlotRestriction(nextPartitionIdx, 0L, null, false));
              return ProcessContinuation.resume();
            } else {
              tracker.tryClaim(new BackfillSlotRestriction(nextPartitionIdx, 0L, null, true));
              return ProcessContinuation.stop();
            }
          }

          Document doc = cursor.next();
          BsonValue docId = doc.toBsonDocument().get("_id");
          String nextIdJson =
              docId != null
                  ? new BsonDocument("_id", docId).toJson(CANONICAL_JSON_SETTINGS)
                  : null;

          if (!tracker.tryClaim(
              new BackfillSlotRestriction(
                  partitionIdx, currentOffset + docsInSlice + 1, nextIdJson, false))) {
            closeCursorForPartition(partitionKey);
            return ProcessContinuation.stop();
          }

          DocumentWithMetadata item =
              partition.getTimestampSortKey() != null
                  ? DocumentWithMetadata.backfillEvent(
                      doc,
                      partition.getSourceCollection(),
                      partition.getTargetCollection(),
                      partition.getTimestampSortKey())
                  : DocumentWithMetadata.of(
                      doc,
                      partition.getSourceCollection(),
                      partition.getTargetCollection());

          receiver.output(item);
          backfillDocumentsRead.inc();
          docsInSlice++;
          lastSeenIdJson = nextIdJson;
          cursorHolder.setLastSeenIdJson(lastSeenIdJson);
        }
      } catch (MongoCursorNotFoundException | MongoSocketException mse) {
        backfillReadErrors.inc();
        LOG.warn(
            "Cached cursor disconnected/expired for partition {} in slot {}: {}."
                + " Resuming from lastSeenId in 500ms.",
            partition,
            slotTask.getSlotId(),
            mse.getMessage());
        closeCursorForPartition(partitionKey);
        return ProcessContinuation.resume().withResumeDelay(Duration.millis(500));
      } catch (MongoException me) {
        backfillReadErrors.inc();
        LOG.warn(
            "Transient MongoDB exception reading partition {} in slot {}: {}. Resuming from lastSeenId in 1s.",
            partition,
            slotTask.getSlotId(),
            me.getMessage());
        closeCursorForPartition(partitionKey);
        return ProcessContinuation.resume().withResumeDelay(Duration.millis(1000));
      } catch (Exception e) {
        backfillReadErrors.inc();
        LOG.error(
            "Unexpected error reading partition {} in slot {}: {}. Failing bundle.",
            partition,
            slotTask.getSlotId(),
            e.getMessage(),
            e);
        closeCursorForPartition(partitionKey);
        throw new RuntimeException("Backfill read failed for partition " + partition, e);
      }

      return ProcessContinuation.resume();
    }

    private void evictExpiredCursors() {
      if (cursorCache != null) {
        cursorCache.entrySet().removeIf(entry -> {
          if (entry.getValue().isExpired(CURSOR_EXPIRATION_TIMEOUT_MS)) {
            entry.getValue().close();
            return true;
          }
          return false;
        });
      }
    }

    private void closeCursorForPartition(String partitionKey) {
      if (cursorCache != null && partitionKey != null) {
        ProcessBackfillPartitionFn.PartitionCursorHolder holder = cursorCache.remove(partitionKey);
        if (holder != null) {
          holder.close();
        }
      }
    }

    @Teardown
    public void teardown() {
      if (cursorCache != null) {
        for (ProcessBackfillPartitionFn.PartitionCursorHolder holder : cursorCache.values()) {
          holder.close();
        }
        cursorCache.clear();
      }
      if (clientCache != null) {
        clientCache.clear();
      }
    }
  }

  /**
   * PTransform that reads backfill partitions using virtual concurrency slot sharding to strictly
   * bound the number of active cursors against the source database.
   */
  public static class ReadPartitions
      extends PTransform<PBegin, PCollection<DocumentWithMetadata>> {

    public static final int DEFAULT_MAX_CONCURRENT_READS = 128;

    private final List<BackfillPartition> partitions;
    private final int maxConcurrentReads;
    private final SerializableFunction<String, MongoClient> clientFactory;

    public ReadPartitions(List<BackfillPartition> partitions) {
      this(partitions, DEFAULT_MAX_CONCURRENT_READS, MongoDbTransforms::getOrCreateMongoClient);
    }

    public ReadPartitions(List<BackfillPartition> partitions, int maxConcurrentReads) {
      this(partitions, maxConcurrentReads, MongoDbTransforms::getOrCreateMongoClient);
    }

    public ReadPartitions(
        List<BackfillPartition> partitions,
        SerializableFunction<String, MongoClient> clientFactory) {
      this(partitions, DEFAULT_MAX_CONCURRENT_READS, clientFactory);
    }

    public ReadPartitions(
        List<BackfillPartition> partitions,
        int maxConcurrentReads,
        SerializableFunction<String, MongoClient> clientFactory) {
      this.partitions = partitions;
      this.maxConcurrentReads =
          maxConcurrentReads > 0 ? maxConcurrentReads : DEFAULT_MAX_CONCURRENT_READS;
      this.clientFactory = clientFactory;
    }

    @Override
    public PCollection<DocumentWithMetadata> expand(PBegin input) {
      if (partitions == null || partitions.isEmpty()) {
        return input
            .apply(
                "EmptyBackfillSlotTasks",
                Create.empty(SerializableCoder.of(BackfillSlotTask.class)))
            .apply("ReshuffleBackfillEmptySlotTasks", Reshuffle.viaRandomKey())
            .apply("ProcessEmptyBackfillSlot", ParDo.of(new ProcessBackfillSlotFn(clientFactory)))
            .setCoder(DocumentWithMetadataCoder.of());
      }

      List<BackfillSlotTask> slotTasks =
          BackfillSlotTask.distribute(partitions, maxConcurrentReads);

      return input
          .apply(
              "CreateBackfillSlotTasks",
              Create.of(slotTasks).withCoder(SerializableCoder.of(BackfillSlotTask.class)))
          .apply("ReshuffleBackfillSlotTasks", Reshuffle.viaRandomKey())
          .apply("ReadBackfillSlotTasks", ParDo.of(new ProcessBackfillSlotFn(clientFactory)))
          .setCoder(DocumentWithMetadataCoder.of());
    }
  }
}
