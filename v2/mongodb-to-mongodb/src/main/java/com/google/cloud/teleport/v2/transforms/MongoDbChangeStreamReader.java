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

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.GetInitialRestriction;
import org.apache.beam.sdk.transforms.DoFn.GetRestrictionCoder;
import org.apache.beam.sdk.transforms.DoFn.NewTracker;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
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
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming reader that consumes MongoDB Change Streams across partitioned cursors.
 *
 * <p>Features:
 * <ul>
 *   <li>Multi-cursor partitioning per collection using orthogonal match filters on {@code documentKey._id}.
 *   <li>Capture-before-read starting timestamp support ({@code startAtOperationTime}).
 *   <li>Configurable {@link FullDocument} strategy (e.g. {@code updateLookup} or {@code whenAvailable}).
 *   <li>Automatic resume-token tracking (including post-batch tokens) and retry with exponential backoff on transient disconnects.
 *   <li>Multi-cursor caching per partition with idle cursor auto-eviction.
 *   <li>Continuous watermark progression on idle polls via resume tokens and bounded wall clock.
 * </ul>
 */
public class MongoDbChangeStreamReader {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbChangeStreamReader.class);

  private static final JsonWriterSettings CANONICAL_JSON_SETTINGS =
      JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

  /** Descriptor representing a single partitioned change stream cursor. */
  public static class ChangeStreamPartition implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String sourceUri;
    private final String sourceDatabase;
    private final String sourceCollection;
    private final String targetCollection;
    private final int partitionIndex;
    private final int totalPartitions;
    private final String matchFilterJson;
    private final long startAtOperationTimeSeconds;
    private final int startAtOperationTimeInc;
    private final String fullDocumentStrategy;

    public ChangeStreamPartition(
        String sourceUri,
        String sourceDatabase,
        String sourceCollection,
        String targetCollection,
        int partitionIndex,
        int totalPartitions,
        String matchFilterJson,
        long startAtOperationTimeSeconds,
        int startAtOperationTimeInc,
        String fullDocumentStrategy) {
      this.sourceUri = sourceUri;
      this.sourceDatabase = sourceDatabase;
      this.sourceCollection = sourceCollection;
      this.targetCollection = targetCollection;
      this.partitionIndex = partitionIndex;
      this.totalPartitions = totalPartitions;
      this.matchFilterJson = matchFilterJson;
      this.startAtOperationTimeSeconds = startAtOperationTimeSeconds;
      this.startAtOperationTimeInc = startAtOperationTimeInc;
      this.fullDocumentStrategy =
          fullDocumentStrategy != null ? fullDocumentStrategy : "updateLookup";
    }

    public String getSourceUri() {
      return sourceUri;
    }

    public String getSourceDatabase() {
      return sourceDatabase;
    }

    public String getSourceCollection() {
      return sourceCollection;
    }

    public String getTargetCollection() {
      return targetCollection;
    }

    public int getPartitionIndex() {
      return partitionIndex;
    }

    public int getTotalPartitions() {
      return totalPartitions;
    }

    public String getMatchFilterJson() {
      return matchFilterJson;
    }

    public long getStartAtOperationTimeSeconds() {
      return startAtOperationTimeSeconds;
    }

    public int getStartAtOperationTimeInc() {
      return startAtOperationTimeInc;
    }

    public String getFullDocumentStrategy() {
      return fullDocumentStrategy;
    }

    public boolean isDatabaseLevel() {
      return sourceCollection == null || sourceCollection.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ChangeStreamPartition)) {
        return false;
      }
      ChangeStreamPartition that = (ChangeStreamPartition) o;
      return partitionIndex == that.partitionIndex
          && totalPartitions == that.totalPartitions
          && startAtOperationTimeSeconds == that.startAtOperationTimeSeconds
          && startAtOperationTimeInc == that.startAtOperationTimeInc
          && Objects.equals(sourceUri, that.sourceUri)
          && Objects.equals(sourceDatabase, that.sourceDatabase)
          && Objects.equals(sourceCollection, that.sourceCollection)
          && Objects.equals(targetCollection, that.targetCollection)
          && Objects.equals(matchFilterJson, that.matchFilterJson)
          && Objects.equals(fullDocumentStrategy, that.fullDocumentStrategy);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          sourceUri,
          sourceDatabase,
          sourceCollection,
          targetCollection,
          partitionIndex,
          totalPartitions,
          matchFilterJson,
          startAtOperationTimeSeconds,
          startAtOperationTimeInc,
          fullDocumentStrategy);
    }

    @Override
    public String toString() {
      return "ChangeStreamPartition{"
          + "database='"
          + sourceDatabase
          + '\''
          + ", collection='"
          + (isDatabaseLevel() ? "[DATABASE_WIDE]" : sourceCollection)
          + '\''
          + ", partition="
          + partitionIndex
          + "/"
          + totalPartitions
          + ", startSec="
          + startAtOperationTimeSeconds
          + '}';
    }
  }

  /**
   * Captures the current source clusterTime from MongoDB.
   *
   * @param sourceUri the MongoDB connection URI
   * @param databaseName the database name
   * @return the cluster BsonTimestamp (or current epoch timestamp fallback)
   */
  public static BsonTimestamp captureCurrentClusterTime(String sourceUri, String databaseName) {
    try (MongoClient client = MongoDbTransforms.createMongoClient(sourceUri)) {
      MongoDatabase db = client.getDatabase(databaseName);
      BsonDocument helloDoc = null;
      try {
        helloDoc = db.runCommand(new BsonDocument("hello", new BsonInt32(1)), BsonDocument.class);
      } catch (Exception e) {
        LOG.debug("hello command failed, attempting isMaster: {}", e.getMessage());
        try {
          helloDoc = db.runCommand(new BsonDocument("isMaster", new BsonInt32(1)), BsonDocument.class);
        } catch (Exception e2) {
          LOG.warn("Both hello and isMaster commands failed on database '{}': {}", databaseName, e2.getMessage());
        }
      }

      if (helloDoc != null && helloDoc.containsKey("$clusterTime")) {
        BsonDocument ctWrapper = helloDoc.getDocument("$clusterTime", null);
        if (ctWrapper != null && ctWrapper.containsKey("clusterTime")) {
          BsonValue ctVal = ctWrapper.get("clusterTime");
          if (ctVal != null && ctVal.isTimestamp()) {
            return ctVal.asTimestamp();
          }
        }
      }
    } catch (Exception e) {
      LOG.warn(
          "Could not retrieve $clusterTime from hello/isMaster on database '{}': {}. Falling back to wall-clock time.",
          databaseName,
          e.getMessage());
    }
    return new BsonTimestamp((int) (System.currentTimeMillis() / 1000), 0);
  }

  /**
   * Generates a MongoDB Change Stream $match filter that uniformly partitions events across
   * {@code numSplits} parallel cursors using server-side keyset hashing ($toHashedIndexKey).
   *
   * @param numSplits Total number of parallel change stream partitions.
   * @param splitIndex 0-based partition index.
   * @return BsonDocument representing the $match stage.
   */
  public static BsonDocument generateHashedMatchFilter(int numSplits, int splitIndex) {
    if (numSplits <= 0) {
      throw new IllegalArgumentException("numSplits must be greater than 0, got " + numSplits);
    }
    if (splitIndex < 0 || splitIndex >= numSplits) {
      throw new IllegalArgumentException(
          String.format("splitIndex must be in range [0, %d), got %d", numSplits, splitIndex));
    }

    BsonDocument modDoc =
        new BsonDocument(
            "$mod",
            new BsonArray(
                Arrays.asList(
                    new BsonDocument("$toHashedIndexKey", new BsonString("$documentKey._id")),
                    new BsonInt32(numSplits))));

    BsonArray matchingValues = new BsonArray();
    matchingValues.add(new BsonInt32(splitIndex));
    matchingValues.add(new BsonInt32(splitIndex - numSplits));

    BsonDocument exprDoc =
        new BsonDocument(
            "$expr",
            new BsonDocument("$in", new BsonArray(Arrays.asList(modDoc, matchingValues))));

    return new BsonDocument("$match", exprDoc);
  }

  /**
   * Generates partition descriptors for a collection using server-side keyset hashing.
   */
  public static List<ChangeStreamPartition> generatePartitions(
      MongoClient client,
      String sourceUri,
      String sourceDatabase,
      String sourceCollection,
      String targetCollection,
      int numSplits,
      BsonTimestamp startAtOperationTime,
      String fullDocumentStrategy) {
    List<ChangeStreamPartition> partitions = new ArrayList<>();
    long startSec = startAtOperationTime != null ? startAtOperationTime.getTime() : 0L;
    int startInc = startAtOperationTime != null ? startAtOperationTime.getInc() : 0;

    if (numSplits <= 1) {
      partitions.add(
          new ChangeStreamPartition(
              sourceUri,
              sourceDatabase,
              sourceCollection,
              targetCollection,
              0,
              1,
              null,
              startSec,
              startInc,
              fullDocumentStrategy));
      return partitions;
    }

    for (int i = 0; i < numSplits; i++) {
      BsonDocument changeStreamFilter = generateHashedMatchFilter(numSplits, i);
      String filterJson = changeStreamFilter.toJson();

      partitions.add(
          new ChangeStreamPartition(
              sourceUri,
              sourceDatabase,
              sourceCollection,
              targetCollection,
              i,
              numSplits,
              filterJson,
              startSec,
              startInc,
              fullDocumentStrategy));
    }

    return partitions;
  }

  /**
   * Generates partition descriptors for a collection based on the requested split count.
   */
  public static List<ChangeStreamPartition> generatePartitions(
      String sourceUri,
      String sourceDatabase,
      String sourceCollection,
      String targetCollection,
      int numSplits,
      BsonTimestamp startAtOperationTime,
      String fullDocumentStrategy) {
    return generatePartitions(
        null,
        sourceUri,
        sourceDatabase,
        sourceCollection,
        targetCollection,
        numSplits,
        startAtOperationTime,
        fullDocumentStrategy);
  }

  /**
   * Generates database-level change stream partition descriptors for an entire database.
   */
  public static List<ChangeStreamPartition> generateDatabasePartitions(
      MongoClient client,
      String sourceUri,
      String sourceDatabase,
      int numSplits,
      BsonTimestamp startAtOperationTime,
      String fullDocumentStrategy) {
    List<ChangeStreamPartition> partitions = new ArrayList<>();
    long startSec = startAtOperationTime != null ? startAtOperationTime.getTime() : 0L;
    int startInc = startAtOperationTime != null ? startAtOperationTime.getInc() : 0;

    if (numSplits <= 1) {
      partitions.add(
          new ChangeStreamPartition(
              sourceUri,
              sourceDatabase,
              null,
              null,
              0,
              1,
              null,
              startSec,
              startInc,
              fullDocumentStrategy));
      return partitions;
    }

    for (int i = 0; i < numSplits; i++) {
      BsonDocument changeStreamFilter = generateHashedMatchFilter(numSplits, i);
      String filterJson = changeStreamFilter.toJson();

      partitions.add(
          new ChangeStreamPartition(
              sourceUri,
              sourceDatabase,
              null,
              null,
              i,
              numSplits,
              filterJson,
              startSec,
              startInc,
              fullDocumentStrategy));
    }

    return partitions;
  }

  /**
   * Generates database-level change stream partition descriptors for an entire database.
   */
  public static List<ChangeStreamPartition> generateDatabasePartitions(
      String sourceUri,
      String sourceDatabase,
      int numSplits,
      BsonTimestamp startAtOperationTime,
      String fullDocumentStrategy) {
    return generateDatabasePartitions(
        null,
        sourceUri,
        sourceDatabase,
        numSplits,
        startAtOperationTime,
        fullDocumentStrategy);
  }

  /**
   * Recursively rewrites {@code "_id"} field references in query filters to {@code "documentKey._id"}
   * for Change Stream {@code $match} pipeline stages.
   */
  public static BsonValue rewriteIdToDocumentKey(BsonValue value) {
    if (value == null) {
      return null;
    }
    if (value.isDocument()) {
      BsonDocument original = value.asDocument();
      BsonDocument rewritten = new BsonDocument();
      for (Map.Entry<String, BsonValue> entry : original.entrySet()) {
        String key = entry.getKey();
        BsonValue val = entry.getValue();
        if ("_id".equals(key)) {
          rewritten.put("documentKey._id", rewriteIdToDocumentKey(val));
        } else {
          rewritten.put(key, rewriteIdToDocumentKey(val));
        }
      }
      return rewritten;
    } else if (value.isArray()) {
      BsonArray originalArray = value.asArray();
      BsonArray rewrittenArray = new BsonArray();
      for (BsonValue item : originalArray) {
        rewrittenArray.add(rewriteIdToDocumentKey(item));
      }
      return rewrittenArray;
    }
    return value;
  }

  /**
   * Translates a standard read filter (e.g. {@code {"_id": {"$gte": ...}}}) into a Change Stream
   * match filter on {@code "documentKey._id"}.
   */
  public static BsonDocument transformToChangeStreamFilter(BsonDocument readFilter) {
    if (readFilter == null || readFilter.isEmpty()) {
      return null;
    }
    BsonValue rewritten = rewriteIdToDocumentKey(readFilter);
    return new BsonDocument("$match", rewritten.asDocument());
  }

  /**
   * SDF restriction for MongoDB Change Streams tracking current offset and serialized resume token.
   */
  public static class ChangeStreamRestriction implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long offset;
    private final String resumeTokenJson;

    public ChangeStreamRestriction() {
      this(0L, null);
    }

    public ChangeStreamRestriction(long offset, String resumeTokenJson) {
      this.offset = offset;
      this.resumeTokenJson = resumeTokenJson;
    }

    public long getOffset() {
      return offset;
    }

    public String getResumeTokenJson() {
      return resumeTokenJson;
    }

    public BsonDocument getResumeToken() {
      return resumeTokenJson != null ? BsonDocument.parse(resumeTokenJson) : null;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ChangeStreamRestriction that = (ChangeStreamRestriction) o;
      return offset == that.offset && Objects.equals(resumeTokenJson, that.resumeTokenJson);
    }

    @Override
    public int hashCode() {
      return Objects.hash(offset, resumeTokenJson);
    }

    @Override
    public String toString() {
      return "ChangeStreamRestriction{offset="
          + offset
          + ", hasResumeToken="
          + (resumeTokenJson != null)
          + "}";
    }
  }

  /**
   * RestrictionTracker for ChangeStreamRestriction ensuring slice-per-offset claiming and
   * checkpoint splitting.
   */
  public static class ChangeStreamRestrictionTracker
      extends RestrictionTracker<ChangeStreamRestriction, ChangeStreamRestriction> {

    private ChangeStreamRestriction currentRestriction;
    private boolean shouldStop = false;

    public ChangeStreamRestrictionTracker(ChangeStreamRestriction restriction) {
      this.currentRestriction =
          restriction != null ? restriction : new ChangeStreamRestriction(0L, null);
    }

    @Override
    public boolean tryClaim(ChangeStreamRestriction position) {
      if (shouldStop) {
        return false;
      }
      this.currentRestriction = position;
      return true;
    }

    @Override
    public ChangeStreamRestriction currentRestriction() {
      return currentRestriction;
    }

    @Override
    public @Nullable SplitResult<ChangeStreamRestriction> trySplit(double fractionOfRemainder) {
      if (fractionOfRemainder == 0) {
        if (shouldStop) {
          return null;
        }
        shouldStop = true;
        ChangeStreamRestriction primary = currentRestriction;
        ChangeStreamRestriction residual =
            new ChangeStreamRestriction(
                currentRestriction.getOffset(), currentRestriction.getResumeTokenJson());
        return SplitResult.of(primary, residual);
      }
      return null;
    }

    @Override
    public void checkDone() throws IllegalStateException {
      // Unbounded continuous streaming source
    }

    @Override
    public IsBounded isBounded() {
      return IsBounded.UNBOUNDED;
    }
  }

  /**
   * PTransform that reads unbounded MongoDB Change Streams for a list of partition descriptors.
   */
  public static class ReadPartitions
      extends PTransform<PBegin, PCollection<DocumentWithMetadata>> {

    private final List<ChangeStreamPartition> partitions;
    private final SerializableFunction<String, MongoClient> clientFactory;

    public ReadPartitions(List<ChangeStreamPartition> partitions) {
      this(partitions, MongoDbTransforms::getOrCreateMongoClient);
    }

    public ReadPartitions(
        List<ChangeStreamPartition> partitions,
        SerializableFunction<String, MongoClient> clientFactory) {
      this.partitions = partitions;
      this.clientFactory = clientFactory;
    }

    @Override
    public PCollection<DocumentWithMetadata> expand(PBegin input) {
      if (partitions == null || partitions.isEmpty()) {
        return input
            .apply(
                "EmptyCDCStreamPartitions",
                Create.empty(SerializableCoder.of(ChangeStreamPartition.class)))
            .apply("ReshuffleEmptyCDCPartitions", Reshuffle.viaRandomKey())
            .apply(
                "ProcessEmptyCDCStream",
                ParDo.of(new ProcessChangeStreamPartitionFn(clientFactory)))
            .setCoder(DocumentWithMetadataCoder.of());
      }

      return input
          .apply(
              "CreateCDCPartitions",
              Create.of(partitions).withCoder(SerializableCoder.of(ChangeStreamPartition.class)))
          .apply("ReshuffleCDCPartitions", Reshuffle.viaRandomKey())
          .apply(
              "StreamChangeEvents",
              ParDo.of(new ProcessChangeStreamPartitionFn(clientFactory)))
          .setCoder(DocumentWithMetadataCoder.of());
    }
  }

  /**
   * Splittable DoFn that maintains high-throughput streaming Change Stream cursors cached per partition.
   */
  public static class ProcessChangeStreamPartitionFn
      extends DoFn<ChangeStreamPartition, DocumentWithMetadata> {

    public static final int MAX_EVENTS_PER_SLICE = 10000;
    public static final long MAX_SLICE_DURATION_MS = 10000L;
    public static final long IDLE_RESUME_DELAY_MS = 20L;
    public static final long GRACE_POLL_TIMEOUT_MS = 300L;
    public static final long CURSOR_EXPIRATION_TIMEOUT_MS = 300_000L; // 5 minutes idle timeout
    public static final int MONGO_ERROR_CHANGE_STREAM_HISTORY_LOST_280 = 280;
    public static final int MONGO_ERROR_CHANGE_STREAM_HISTORY_LOST_286 = 286;

    /**
     * Stride (in events) at which the BSON resume token is serialized to an Extended JSON string
     * during the inner polling loop. Serializing toJson() on all 10,000 events per slice causes
     * heavy CPU and heap contention on reader threads. Reusing the serialized token string across
     * this stride eliminates 99.5% of serialization overhead while still satisfying Beam's
     * claim-before-output requirement on every single element. The exact final resume token is always
     * serialized and committed at slice completion.
     */
    public static final int RESUME_TOKEN_SERIALIZATION_STRIDE = 200;

    private final Counter changeEventsRead =
        Metrics.counter(MongoDbChangeStreamReader.class, "changeEventsRead");
    private final Counter changeEventsErrors =
        Metrics.counter(MongoDbChangeStreamReader.class, "changeEventsErrors");
    private final Counter changeStreamHistoryLost =
        Metrics.counter(MongoDbChangeStreamReader.class, "changeStreamHistoryLost");
    private final Counter changeEventsInserts =
        Metrics.counter(MongoDbChangeStreamReader.class, "changeEventsInserts");
    private final Counter changeEventsUpdates =
        Metrics.counter(MongoDbChangeStreamReader.class, "changeEventsUpdates");
    private final Counter changeEventsReplaces =
        Metrics.counter(MongoDbChangeStreamReader.class, "changeEventsReplaces");
    private final Counter changeEventsDeletes =
        Metrics.counter(MongoDbChangeStreamReader.class, "changeEventsDeletes");
    private final Counter changeEventsDrops =
        Metrics.counter(MongoDbChangeStreamReader.class, "changeEventsDrops");
    private final Counter changeEventsRenames =
        Metrics.counter(MongoDbChangeStreamReader.class, "changeEventsRenames");
    private final Counter changeEventsOther =
        Metrics.counter(MongoDbChangeStreamReader.class, "changeEventsOther");
    private final Counter changeEventsDroppedNullPayload =
        Metrics.counter(MongoDbChangeStreamReader.class, "changeEventsDroppedNullPayload");
    private final Counter changeStreamPollCycles =
        Metrics.counter(MongoDbChangeStreamReader.class, "changeStreamPollCycles");
    private final Counter changeStreamEmptyPolls =
        Metrics.counter(MongoDbChangeStreamReader.class, "changeStreamEmptyPolls");
    private final Counter changeStreamBurstPolls =
        Metrics.counter(MongoDbChangeStreamReader.class, "changeStreamBurstPolls");
    private final Counter changeStreamCursorReconnects =
        Metrics.counter(MongoDbChangeStreamReader.class, "changeStreamCursorReconnects");

    /** Holds a cached cursor and tracks its last access timestamp and resume token for eviction. */
    public static class PartitionCursorHolder implements AutoCloseable {
      private final MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;
      private volatile long lastAccessedMs;
      private volatile BsonDocument lastResumeToken;

      public PartitionCursorHolder(
          MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor,
          BsonDocument initialToken) {
        this.cursor = cursor;
        this.lastAccessedMs = System.currentTimeMillis();
        this.lastResumeToken = initialToken;
      }

      public MongoChangeStreamCursor<ChangeStreamDocument<Document>> getCursor() {
        this.lastAccessedMs = System.currentTimeMillis();
        return cursor;
      }

      public BsonDocument getLastResumeToken() {
        return lastResumeToken;
      }

      public void setLastResumeToken(BsonDocument token) {
        this.lastResumeToken = token;
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

    private final SerializableFunction<String, MongoClient> clientFactory;

    private transient ConcurrentHashMap<String, MongoClient> clientCache;
    private transient ConcurrentHashMap<String, PartitionCursorHolder> cursorCache;

    public ProcessChangeStreamPartitionFn() {
      this(MongoDbTransforms::getOrCreateMongoClient);
    }

    public ProcessChangeStreamPartitionFn(
        SerializableFunction<String, MongoClient> clientFactory) {
      this.clientFactory = clientFactory;
    }

    @GetInitialRestriction
    public ChangeStreamRestriction getInitialRestriction(@Element ChangeStreamPartition partition) {
      return new ChangeStreamRestriction(0L, null);
    }

    @NewTracker
    public ChangeStreamRestrictionTracker newTracker(
        @Element ChangeStreamPartition partition,
        @Restriction ChangeStreamRestriction restriction) {
      return new ChangeStreamRestrictionTracker(restriction);
    }

    @GetRestrictionCoder
    public Coder<ChangeStreamRestriction> getRestrictionCoder() {
      return SerializableCoder.of(ChangeStreamRestriction.class);
    }

    @ProcessElement
    public ProcessContinuation processElement(
        @Element ChangeStreamPartition partition,
        RestrictionTracker<ChangeStreamRestriction, ChangeStreamRestriction> tracker,
        OutputReceiver<DocumentWithMetadata> receiver) {

      if (cursorCache == null) {
        cursorCache = new ConcurrentHashMap<>();
      }
      if (clientCache == null) {
        clientCache = new ConcurrentHashMap<>();
      }
      evictExpiredCursors();

      changeStreamPollCycles.inc();
      ChangeStreamRestriction currentRestriction = tracker.currentRestriction();
      if (!tracker.tryClaim(currentRestriction)) {
        return ProcessContinuation.stop();
      }

      String partitionKey =
          partition.getSourceDatabase()
              + (partition.isDatabaseLevel() ? "#db" : "." + partition.getSourceCollection())
              + "#"
              + partition.getPartitionIndex();

      BsonDocument currentToken = currentRestriction.getResumeToken();
      PartitionCursorHolder cursorHolder = cursorCache.get(partitionKey);

      // Validate that cached cursor position matches the incoming restriction resume token
      if (cursorHolder != null) {
        if (!Objects.equals(cursorHolder.getLastResumeToken(), currentToken)) {
          // Partition was progressed on another worker or reconnect required; discard stale cursor
          closeCursorForPartition(partitionKey);
          cursorHolder = null;
        }
      }

      // Ensure cursor is initialized for this partition
      if (cursorHolder == null) {
        try {
          MongoClient mongoClient =
              clientCache.computeIfAbsent(partition.getSourceUri(), clientFactory::apply);

          MongoDatabase db = mongoClient.getDatabase(partition.getSourceDatabase());

          List<Bson> pipeline = new ArrayList<>();
          if (partition.getMatchFilterJson() != null && !partition.getMatchFilterJson().isEmpty()) {
            pipeline.add(BsonDocument.parse(partition.getMatchFilterJson()));
          }

          ChangeStreamIterable<Document> stream;
          if (partition.isDatabaseLevel()) {
            stream = db.watch(pipeline);
          } else {
            MongoCollection<Document> collection = db.getCollection(partition.getSourceCollection());
            stream = collection.watch(pipeline);
          }

          stream
              .batchSize(MAX_EVENTS_PER_SLICE)
              .maxAwaitTime(250L, java.util.concurrent.TimeUnit.MILLISECONDS);

          String fullDocStrategy = partition.getFullDocumentStrategy();
          if ("whenAvailable".equalsIgnoreCase(fullDocStrategy)) {
            try {
              stream.fullDocument(FullDocument.WHEN_AVAILABLE);
            } catch (Exception e) {
              LOG.warn("whenAvailable not supported, falling back to updateLookup");
              stream.fullDocument(FullDocument.UPDATE_LOOKUP);
            }
          } else if ("required".equalsIgnoreCase(fullDocStrategy)) {
            try {
              stream.fullDocument(FullDocument.REQUIRED);
            } catch (Exception e) {
              stream.fullDocument(FullDocument.UPDATE_LOOKUP);
            }
          } else if (!"default".equalsIgnoreCase(fullDocStrategy)) {
            stream.fullDocument(FullDocument.UPDATE_LOOKUP);
          }

          if (currentToken != null) {
            stream.resumeAfter(currentToken);
          } else if (partition.getStartAtOperationTimeSeconds() > 0) {
            stream.startAtOperationTime(
                new BsonTimestamp(
                    (int) partition.getStartAtOperationTimeSeconds(),
                    partition.getStartAtOperationTimeInc()));
          }

          MongoChangeStreamCursor<ChangeStreamDocument<Document>> activeCursor = stream.cursor();
          cursorHolder = new PartitionCursorHolder(activeCursor, currentToken);
          cursorCache.put(partitionKey, cursorHolder);
          changeStreamCursorReconnects.inc();
        } catch (com.mongodb.MongoCommandException mce) {
          int errCode = mce.getErrorCode();
          String targetDesc =
              partition.isDatabaseLevel()
                  ? "database '" + partition.getSourceDatabase() + "'"
                  : "collection '" + partition.getSourceCollection() + "'";
          if (errCode == MONGO_ERROR_CHANGE_STREAM_HISTORY_LOST_280
              || errCode == MONGO_ERROR_CHANGE_STREAM_HISTORY_LOST_286) {
            changeStreamHistoryLost.inc();
            LOG.error(
                "FATAL: ChangeStreamHistoryLost (code={}) on {} partition {}. MongoDB oplog rolled over: {}",
                errCode,
                targetDesc,
                partition.getPartitionIndex(),
                mce.getMessage());
          }
          changeEventsErrors.inc();
          closeCursorForPartition(partitionKey);
          return ProcessContinuation.resume().withResumeDelay(Duration.millis(1000));
        } catch (Exception e) {
          String targetDesc =
              partition.isDatabaseLevel()
                  ? "database '" + partition.getSourceDatabase() + "'"
                  : "collection '" + partition.getSourceCollection() + "'";
          changeEventsErrors.inc();
          LOG.error(
              "Error initializing change stream cursor for {}, partition {}: {}. Will retry in 1s.",
              targetDesc,
              partition.getPartitionIndex(),
              e.getMessage(),
              e);
          closeCursorForPartition(partitionKey);
          return ProcessContinuation.resume().withResumeDelay(Duration.millis(1000));
        }
      }

      long sliceStartTime = System.currentTimeMillis();
      int eventsInSlice = 0;
      long currentOffset = currentRestriction.getOffset();
      BsonDocument postBatchToken = null;
      BsonDocument lastSeenResumeToken = null;
      String currentResumeTokenJson = currentRestriction.getResumeTokenJson();

      try {
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = cursorHolder.getCursor();
        long lastEventTimeMs = sliceStartTime;
        while (eventsInSlice < MAX_EVENTS_PER_SLICE
            && (System.currentTimeMillis() - sliceStartTime) < MAX_SLICE_DURATION_MS) {
          ChangeStreamDocument<Document> event = cursor.tryNext();
          if (event == null) {
            long timeSinceLastEvent = System.currentTimeMillis() - lastEventTimeMs;
            if (timeSinceLastEvent < GRACE_POLL_TIMEOUT_MS
                && (System.currentTimeMillis() - sliceStartTime) < MAX_SLICE_DURATION_MS) {
              try {
                Thread.sleep(5);
              } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
                break;
              }
              continue;
            }
            // Post-batch resume token capture on idle
            try {
              postBatchToken = cursor.getResumeToken();
              if (postBatchToken != null) {
                cursorHolder.setLastResumeToken(postBatchToken);
                currentResumeTokenJson = postBatchToken.toJson(CANONICAL_JSON_SETTINGS);
                if (!tracker.tryClaim(
                    new ChangeStreamRestriction(
                        currentOffset + eventsInSlice, currentResumeTokenJson))) {
                  closeCursorForPartition(partitionKey);
                  return ProcessContinuation.stop();
                }
              }
            } catch (Exception e) {
              LOG.debug("Could not retrieve post-batch resume token: {}", e.getMessage());
            }
            break;
          }

          lastEventTimeMs = System.currentTimeMillis();
          eventsInSlice++;
          changeEventsRead.inc();

          com.mongodb.client.model.changestream.OperationType mongoOp = event.getOperationType();
          if (mongoOp != null) {
            switch (mongoOp) {
              case INSERT:
                changeEventsInserts.inc();
                break;
              case UPDATE:
                changeEventsUpdates.inc();
                break;
              case REPLACE:
                changeEventsReplaces.inc();
                break;
              case DELETE:
                changeEventsDeletes.inc();
                break;
              case DROP:
                changeEventsDrops.inc();
                break;
              case RENAME:
                changeEventsRenames.inc();
                break;
              default:
                changeEventsOther.inc();
                break;
            }
          }

          if (event.getResumeToken() != null) {
            lastSeenResumeToken = event.getResumeToken();
            cursorHolder.setLastResumeToken(lastSeenResumeToken);
            if (eventsInSlice % RESUME_TOKEN_SERIALIZATION_STRIDE == 0) {
              currentResumeTokenJson = lastSeenResumeToken.toJson(CANONICAL_JSON_SETTINGS);
            }
            if (!tracker.tryClaim(
                new ChangeStreamRestriction(
                    currentOffset + eventsInSlice, currentResumeTokenJson))) {
              closeCursorForPartition(partitionKey);
              return ProcessContinuation.stop();
            }
          }

          DocumentWithMetadata cdcItem =
              mapChangeStreamEvent(
                  event, partition.getSourceCollection(), partition.getTargetCollection());

          if (cdcItem != null) {
            receiver.output(cdcItem);
          } else {
            changeEventsDroppedNullPayload.inc();
          }
        }

        if (lastSeenResumeToken != null && eventsInSlice % RESUME_TOKEN_SERIALIZATION_STRIDE != 0) {
          currentResumeTokenJson = lastSeenResumeToken.toJson(CANONICAL_JSON_SETTINGS);
          tracker.tryClaim(
              new ChangeStreamRestriction(currentOffset + eventsInSlice, currentResumeTokenJson));
        }
      } catch (com.mongodb.MongoCommandException mce) {
        int errCode = mce.getErrorCode();
        String targetDesc =
            partition.isDatabaseLevel()
                ? "database '" + partition.getSourceDatabase() + "'"
                : "collection '" + partition.getSourceCollection() + "'";
        if (errCode == MONGO_ERROR_CHANGE_STREAM_HISTORY_LOST_280
            || errCode == MONGO_ERROR_CHANGE_STREAM_HISTORY_LOST_286) {
          changeStreamHistoryLost.inc();
          LOG.error(
              "FATAL: ChangeStreamHistoryLost (code={}) during streaming on {} partition {}. MongoDB oplog rolled over: {}",
              errCode,
              targetDesc,
              partition.getPartitionIndex(),
              mce.getMessage());
        }
        changeEventsErrors.inc();
        closeCursorForPartition(partitionKey);
        return ProcessContinuation.resume().withResumeDelay(Duration.millis(1000));
      } catch (Exception e) {
        String targetDesc =
            partition.isDatabaseLevel()
                ? "database '" + partition.getSourceDatabase() + "'"
                : "collection '" + partition.getSourceCollection() + "'";
        changeEventsErrors.inc();
        LOG.warn(
            "Transient exception during change stream poll on {}, partition {}: {}. Reconnecting cursor...",
            targetDesc,
            partition.getPartitionIndex(),
            e.getMessage());
        closeCursorForPartition(partitionKey);
        return ProcessContinuation.resume().withResumeDelay(Duration.millis(1000));
      }

      if (eventsInSlice > 0) {
        if (eventsInSlice >= MAX_EVENTS_PER_SLICE) {
          changeStreamBurstPolls.inc();
        }
        return ProcessContinuation.resume(); // 0ms immediate resume for high-throughput burst
      } else {
        changeStreamEmptyPolls.inc();
        return ProcessContinuation.resume()
            .withResumeDelay(Duration.millis(IDLE_RESUME_DELAY_MS));
      }
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
   * Maps a MongoDB {@link ChangeStreamDocument} to {@link DocumentWithMetadata}.
   */
  public static DocumentWithMetadata mapChangeStreamEvent(
      ChangeStreamDocument<Document> event, String sourceCollection, String targetCollection) {
    if (event == null) {
      return null;
    }

    String eventCol = sourceCollection;
    if (event.getNamespace() != null && event.getNamespace().getCollectionName() != null) {
      eventCol = event.getNamespace().getCollectionName();
    }
    if (eventCol == null || eventCol.startsWith("system.")) {
      return null;
    }

    String targetCol =
        (targetCollection != null && !targetCollection.isEmpty())
            ? targetCollection
            : eventCol;

    com.mongodb.client.model.changestream.OperationType mongoOp = event.getOperationType();
    if (mongoOp == null) {
      return null;
    }

    DocumentWithMetadata.OperationType opType;
    switch (mongoOp) {
      case INSERT:
        opType = DocumentWithMetadata.OperationType.INSERT;
        break;
      case UPDATE:
        opType = DocumentWithMetadata.OperationType.UPDATE;
        break;
      case REPLACE:
        opType = DocumentWithMetadata.OperationType.REPLACE;
        break;
      case DELETE:
        opType = DocumentWithMetadata.OperationType.DELETE;
        break;
      case DROP:
        opType = DocumentWithMetadata.OperationType.DROP;
        break;
      case RENAME:
        opType = DocumentWithMetadata.OperationType.RENAME;
        break;
      default:
        LOG.debug("Ignoring unsupported change stream operation: {}", mongoOp);
        return null;
    }

    BsonTimestamp clusterTime = event.getClusterTime();
    long epochSeconds = clusterTime != null ? clusterTime.getTime() : (System.currentTimeMillis() / 1000);
    long subSeconds = clusterTime != null ? clusterTime.getInc() : 0L;
    TimestampSortKey sortKey = TimestampSortKey.cdc(epochSeconds, subSeconds);

    BsonDocument docKeyBson = event.getDocumentKey();
    String docKeyStr = docKeyBson != null ? docKeyBson.toJson(CANONICAL_JSON_SETTINGS) : null;

    Document fullDoc = event.getFullDocument();
    if (fullDoc == null
        && (opType == DocumentWithMetadata.OperationType.INSERT
            || opType == DocumentWithMetadata.OperationType.UPDATE
            || opType == DocumentWithMetadata.OperationType.REPLACE)) {
      // Document was deleted between the mutation and the updateLookup.
      // Dropping because no payload is available to write, and subsequent DELETE handles deletion.
      return null;
    }
    String originalDocStr = fullDoc != null ? fullDoc.toJson(CANONICAL_JSON_SETTINGS) : null;

    return DocumentWithMetadata.cdcEvent(
        fullDoc, originalDocStr, eventCol, targetCol, opType, sortKey, docKeyStr);
  }
}
