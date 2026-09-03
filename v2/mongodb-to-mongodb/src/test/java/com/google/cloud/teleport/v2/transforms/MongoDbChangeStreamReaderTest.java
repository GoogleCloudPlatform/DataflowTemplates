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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.transforms.MongoDbChangeStreamReader.ChangeStreamPartition;
import com.google.cloud.teleport.v2.transforms.MongoDbChangeStreamReader.ChangeStreamRestriction;
import com.google.cloud.teleport.v2.transforms.MongoDbChangeStreamReader.ChangeStreamRestrictionTracker;
import com.google.cloud.teleport.v2.transforms.MongoDbChangeStreamReader.ProcessChangeStreamPartitionFn;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link MongoDbChangeStreamReader}. */
@RunWith(JUnit4.class)
public class MongoDbChangeStreamReaderTest {

  private static ChangeStreamDocument<Document> createEvent(
      OperationType op, Document fullDoc, BsonDocument docKey, BsonTimestamp timestamp) {
    return new ChangeStreamDocument<>(
        op != null ? op.getValue() : null,
        new BsonDocument(), // resumeToken
        null, // namespaceDocument
        null, // namespaceType
        null, // destinationNamespaceDocument
        fullDoc,
        null, // fullDocumentBeforeChange
        docKey,
        timestamp,
        null, // updateDescription
        null, // txnNumber
        null, // lsid
        null, // wallTime
        null, // splitEvent
        null  // extraElements
    );
  }

  @Test
  public void testTransformToChangeStreamFilter_mapsIdToDocumentKey() {
    BsonDocument readFilter =
        BsonDocument.parse(
            "{\"_id\": {\"$gte\": {\"$oid\": \"000000000000000000000000\"}, \"$lt\": {\"$oid\":"
                + " \"400000000000000000000000\"}}}");

    BsonDocument changeFilter = MongoDbChangeStreamReader.transformToChangeStreamFilter(readFilter);

    assertNotNull(changeFilter);
    assertTrue(changeFilter.containsKey("$match"));
    BsonDocument matchDoc = changeFilter.getDocument("$match");
    assertTrue(matchDoc.containsKey("documentKey._id"));
    assertEquals(readFilter.get("_id"), matchDoc.get("documentKey._id"));
  }

  @Test
  public void testTransformToChangeStreamFilter_compoundOrAnd() {
    BsonDocument compoundFilter =
        BsonDocument.parse(
            "{\"$or\": ["
                + "  {\"_id\": {\"$gte\": 0, \"$lt\": 100}},"
                + "  {\"$and\": [{\"_id\": {\"$gte\": 200}}, {\"_id\": {\"$lt\": 300}}]}"
                + "]}");

    BsonDocument changeFilter = MongoDbChangeStreamReader.transformToChangeStreamFilter(compoundFilter);

    assertNotNull(changeFilter);
    assertTrue(changeFilter.containsKey("$match"));
    BsonDocument matchDoc = changeFilter.getDocument("$match");
    assertTrue(matchDoc.containsKey("$or"));
    org.bson.BsonArray orArray = matchDoc.getArray("$or");
    assertEquals(2, orArray.size());

    BsonDocument firstBranch = orArray.get(0).asDocument();
    assertTrue(firstBranch.containsKey("documentKey._id"));
    assertTrue(firstBranch.getDocument("documentKey._id").containsKey("$gte"));

    BsonDocument secondBranch = orArray.get(1).asDocument();
    assertTrue(secondBranch.containsKey("$and"));
    org.bson.BsonArray andArray = secondBranch.getArray("$and");
    assertEquals(2, andArray.size());
    assertTrue(andArray.get(0).asDocument().containsKey("documentKey._id"));
    assertTrue(andArray.get(1).asDocument().containsKey("documentKey._id"));
  }

  @Test
  public void testTransformToChangeStreamFilter_nullOrEmpty() {
    assertNull(MongoDbChangeStreamReader.transformToChangeStreamFilter(null));
    assertNull(MongoDbChangeStreamReader.transformToChangeStreamFilter(new BsonDocument()));
  }

  @Test
  public void testGeneratePartitions_singleSplit() {
    BsonTimestamp timestamp = new BsonTimestamp(1724000000, 1);
    List<ChangeStreamPartition> partitions =
        MongoDbChangeStreamReader.generatePartitions(
            "mongodb://localhost:27017",
            "testDb",
            "sourceCol",
            "targetCol",
            1,
            timestamp,
            "updateLookup");

    assertEquals(1, partitions.size());
    ChangeStreamPartition partition = partitions.get(0);
    assertEquals("sourceCol", partition.getSourceCollection());
    assertEquals("targetCol", partition.getTargetCollection());
    assertEquals(0, partition.getPartitionIndex());
    assertEquals(1, partition.getTotalPartitions());
    assertNull(partition.getMatchFilterJson());
    assertEquals(1724000000L, partition.getStartAtOperationTimeSeconds());
    assertEquals(1, partition.getStartAtOperationTimeInc());
    assertEquals("updateLookup", partition.getFullDocumentStrategy());
  }

  @Test
  public void testGenerateHashedMatchFilter_constructsCorrectBsonExpression() {
    BsonDocument filter = MongoDbChangeStreamReader.generateHashedMatchFilter(8, 3);

    assertNotNull(filter);
    assertTrue(filter.containsKey("$match"));
    BsonDocument matchDoc = filter.getDocument("$match");
    assertTrue(matchDoc.containsKey("$expr"));

    BsonDocument exprDoc = matchDoc.getDocument("$expr");
    assertTrue(exprDoc.containsKey("$in"));
    org.bson.BsonArray inArray = exprDoc.getArray("$in");
    assertEquals(2, inArray.size());

    // First element: $mod expression
    BsonDocument modDoc = inArray.get(0).asDocument();
    assertTrue(modDoc.containsKey("$mod"));
    org.bson.BsonArray modArgs = modDoc.getArray("$mod");
    assertEquals(2, modArgs.size());
    BsonDocument toHashedDoc = modArgs.get(0).asDocument();
    assertEquals(new BsonString("$documentKey._id"), toHashedDoc.get("$toHashedIndexKey"));
    assertEquals(new BsonInt32(8), modArgs.get(1));

    // Second element: [3, -5] matching values
    org.bson.BsonArray matchingValues = inArray.get(1).asArray();
    assertEquals(2, matchingValues.size());
    assertEquals(new BsonInt32(3), matchingValues.get(0));
    assertEquals(new BsonInt32(-5), matchingValues.get(1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGenerateHashedMatchFilter_invalidNumSplits_throwsException() {
    MongoDbChangeStreamReader.generateHashedMatchFilter(0, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGenerateHashedMatchFilter_outOfBoundsSplitIndex_throwsException() {
    MongoDbChangeStreamReader.generateHashedMatchFilter(4, 4);
  }

  @Test
  public void testGenerateHashedMatchFilter_mathematicalCompletenessAndDisjointness() {
    int[] testSplitCounts = {2, 4, 8, 16};
    for (int n : testSplitCounts) {
      for (int r = -(n - 1); r <= (n - 1); r++) {
        int matchCount = 0;
        for (int i = 0; i < n; i++) {
          BsonDocument filter = MongoDbChangeStreamReader.generateHashedMatchFilter(n, i);
          org.bson.BsonArray matchingValues =
              filter
                  .getDocument("$match")
                  .getDocument("$expr")
                  .getArray("$in")
                  .get(1)
                  .asArray();

          int val1 = matchingValues.get(0).asInt32().getValue();
          int val2 = matchingValues.get(1).asInt32().getValue();
          if (r == val1 || r == val2) {
            matchCount++;
          }
        }
        assertEquals(
            String.format("Remainder %d for numSplits=%d must match exactly 1 split", r, n),
            1,
            matchCount);
      }
    }
  }

  @Test
  public void testGeneratePartitions_multiSplitHashed() {
    BsonTimestamp timestamp = new BsonTimestamp(1724000000, 1);
    List<ChangeStreamPartition> partitions =
        MongoDbChangeStreamReader.generatePartitions(
            "mongodb://localhost:27017",
            "testDb",
            "sourceCol",
            "targetCol",
            8,
            timestamp,
            "whenAvailable");

    assertEquals(8, partitions.size());
    for (int i = 0; i < partitions.size(); i++) {
      ChangeStreamPartition p = partitions.get(i);
      assertEquals("sourceCol", p.getSourceCollection());
      assertEquals("targetCol", p.getTargetCollection());
      assertEquals(i, p.getPartitionIndex());
      assertEquals(8, p.getTotalPartitions());
      assertEquals("whenAvailable", p.getFullDocumentStrategy());
      assertNotNull(p.getMatchFilterJson());
      assertTrue(p.getMatchFilterJson().contains("$toHashedIndexKey"));
      assertTrue(p.getMatchFilterJson().contains("$documentKey._id"));
    }
  }

  @Test
  public void testMapChangeStreamEvent_insert() {
    Document doc = new Document("_id", "123").append("name", "Bob");
    BsonDocument docKey = new BsonDocument("_id", new BsonString("123"));
    BsonTimestamp timestamp = new BsonTimestamp(1724000000, 5);

    ChangeStreamDocument<Document> event =
        createEvent(OperationType.INSERT, doc, docKey, timestamp);

    DocumentWithMetadata item =
        MongoDbChangeStreamReader.mapChangeStreamEvent(event, "srcCol", "tgtCol");

    assertNotNull(item);
    assertEquals(DocumentWithMetadata.OperationType.INSERT, item.getOperationType());
    assertTrue(item.getOperationType().isUpsert());
    assertEquals("123", item.getId());
    assertEquals(TimestampSortKey.cdc(1724000000L, 5L), item.getTimestampSortKey());
    assertEquals("srcCol", item.getSourceCollection());
    assertEquals("tgtCol", item.getTargetCollection());
  }

  @Test
  public void testMapChangeStreamEvent_delete() {
    BsonDocument docKey = new BsonDocument("_id", new BsonInt32(999));
    BsonTimestamp timestamp = new BsonTimestamp(1724000000, 12);

    ChangeStreamDocument<Document> event =
        createEvent(OperationType.DELETE, null, docKey, timestamp);

    DocumentWithMetadata item =
        MongoDbChangeStreamReader.mapChangeStreamEvent(event, "srcCol", "tgtCol");

    assertNotNull(item);
    assertEquals(DocumentWithMetadata.OperationType.DELETE, item.getOperationType());
    assertTrue(item.getOperationType().isDelete());
    assertNull(item.getDocument());
    assertEquals(999, item.getId());
    assertEquals(TimestampSortKey.cdc(1724000000L, 12L), item.getTimestampSortKey());
  }

  @Test
  public void testMapChangeStreamEvent_update() {
    Document doc = new Document("_id", "456").append("score", 100);
    BsonDocument docKey = new BsonDocument("_id", new BsonString("456"));
    BsonTimestamp timestamp = new BsonTimestamp(1724000000, 8);

    ChangeStreamDocument<Document> event =
        createEvent(OperationType.UPDATE, doc, docKey, timestamp);

    DocumentWithMetadata item =
        MongoDbChangeStreamReader.mapChangeStreamEvent(event, "srcCol", "tgtCol");

    assertNotNull(item);
    assertEquals(DocumentWithMetadata.OperationType.UPDATE, item.getOperationType());
    assertTrue(item.getOperationType().isUpsert());
    assertEquals(TimestampSortKey.cdc(1724000000L, 8L), item.getTimestampSortKey());
  }

  @Test
  public void testMapChangeStreamEvent_replace() {
    Document doc = new Document("_id", "789").append("replaced", true);
    BsonDocument docKey = new BsonDocument("_id", new BsonString("789"));
    BsonTimestamp timestamp = new BsonTimestamp(1724000000, 15);

    ChangeStreamDocument<Document> event =
        createEvent(OperationType.REPLACE, doc, docKey, timestamp);

    DocumentWithMetadata item =
        MongoDbChangeStreamReader.mapChangeStreamEvent(event, "srcCol", "tgtCol");

    assertNotNull(item);
    assertEquals(DocumentWithMetadata.OperationType.REPLACE, item.getOperationType());
    assertTrue(item.getOperationType().isUpsert());
    assertEquals("789", item.getId());
    assertEquals(TimestampSortKey.cdc(1724000000L, 15L), item.getTimestampSortKey());
  }

  @Test
  public void testMapChangeStreamEvent_dropAndRename() {
    BsonTimestamp timestamp = new BsonTimestamp(1724000000, 20);

    ChangeStreamDocument<Document> dropEvent =
        createEvent(OperationType.DROP, null, null, timestamp);
    DocumentWithMetadata dropItem =
        MongoDbChangeStreamReader.mapChangeStreamEvent(dropEvent, "srcCol", "tgtCol");
    assertNotNull(dropItem);
    assertEquals(DocumentWithMetadata.OperationType.DROP, dropItem.getOperationType());

    ChangeStreamDocument<Document> renameEvent =
        createEvent(OperationType.RENAME, null, null, timestamp);
    DocumentWithMetadata renameItem =
        MongoDbChangeStreamReader.mapChangeStreamEvent(renameEvent, "srcCol", "tgtCol");
    assertNotNull(renameItem);
    assertEquals(DocumentWithMetadata.OperationType.RENAME, renameItem.getOperationType());
  }

  @Test
  public void testMapChangeStreamEvent_nullHandling() {
    assertNull(MongoDbChangeStreamReader.mapChangeStreamEvent(null, "s", "t"));

    BsonDocument docKey = new BsonDocument("_id", new BsonString("456"));
    BsonTimestamp timestamp = new BsonTimestamp(1724000000, 8);

    // Update with null fullDocument (document was deleted before updateLookup) should return null
    ChangeStreamDocument<Document> updateEventNullDoc =
        createEvent(OperationType.UPDATE, null, docKey, timestamp);
    assertNull(MongoDbChangeStreamReader.mapChangeStreamEvent(updateEventNullDoc, "srcCol", "tgtCol"));

    // Insert with null fullDocument should return null
    ChangeStreamDocument<Document> insertEventNullDoc =
        createEvent(OperationType.INSERT, null, docKey, timestamp);
    assertNull(MongoDbChangeStreamReader.mapChangeStreamEvent(insertEventNullDoc, "srcCol", "tgtCol"));

    // Replace with null fullDocument should return null
    ChangeStreamDocument<Document> replaceEventNullDoc =
        createEvent(OperationType.REPLACE, null, docKey, timestamp);
    assertNull(MongoDbChangeStreamReader.mapChangeStreamEvent(replaceEventNullDoc, "srcCol", "tgtCol"));
  }

  @Test
  public void testCaptureCurrentClusterTime_fallback() {
    // Calling with an unreachable host should gracefully fallback to wall-clock time
    BsonTimestamp timestamp =
        MongoDbChangeStreamReader.captureCurrentClusterTime(
            "mongodb://invalid-host-for-test:27017/?serverSelectionTimeoutMS=200&connectTimeoutMS=200",
            "testDb");
    assertNotNull(timestamp);
    assertTrue(timestamp.getTime() > 0);
  }

  @Test
  public void testChangeStreamRestriction_gettersAndEquals() {
    ChangeStreamRestriction r1 = new ChangeStreamRestriction(0L, null);
    ChangeStreamRestriction r2 = new ChangeStreamRestriction(0L, null);
    ChangeStreamRestriction r3 = new ChangeStreamRestriction(1L, "{\"_data\": \"abc\"}");

    assertEquals(0L, r1.getOffset());
    assertNull(r1.getResumeTokenJson());
    assertNull(r1.getResumeToken());
    assertEquals(r1, r2);
    assertEquals(r1.hashCode(), r2.hashCode());
    assertFalse(r1.equals(r3));

    assertEquals("{\"_data\": \"abc\"}", r3.getResumeTokenJson());
    assertNotNull(r3.getResumeToken());
    assertEquals(new BsonString("abc"), r3.getResumeToken().get("_data"));
    assertTrue(r3.toString().contains("offset=1"));
  }

  @Test
  public void testChangeStreamRestrictionTracker_claimAndSplit() {
    ChangeStreamRestriction initial = new ChangeStreamRestriction(5L, null);
    ChangeStreamRestrictionTracker tracker = new ChangeStreamRestrictionTracker(initial);

    assertEquals(initial, tracker.currentRestriction());
    assertTrue(tracker.tryClaim(initial));

    BsonDocument token = new BsonDocument("_data", new BsonString("token123"));
    ChangeStreamRestriction nextRestriction =
        new ChangeStreamRestriction(6L, token.toJson());
    assertTrue(tracker.tryClaim(nextRestriction));

    assertEquals(6L, tracker.currentRestriction().getOffset());
    assertNotNull(tracker.currentRestriction().getResumeTokenJson());

    SplitResult<ChangeStreamRestriction> split = tracker.trySplit(0);
    assertNotNull(split);
    assertNotNull(split.getPrimary());
    assertEquals(6L, split.getPrimary().getOffset());
    assertEquals(tracker.currentRestriction().getResumeTokenJson(), split.getPrimary().getResumeTokenJson());
    assertNotNull(split.getResidual());
    assertEquals(6L, split.getResidual().getOffset());
    assertEquals(tracker.currentRestriction().getResumeTokenJson(), split.getResidual().getResumeTokenJson());
    assertFalse(tracker.tryClaim(new ChangeStreamRestriction(7L, null)));
  }

  @Test
  public void testProcessChangeStreamPartitionFn_sdfInitialRestrictionAndTracker() {
    ProcessChangeStreamPartitionFn fn = new ProcessChangeStreamPartitionFn();
    ChangeStreamPartition partition =
        new ChangeStreamPartition(
            "mongodb://localhost:27017",
            "testDb",
            "users",
            "users",
            0,
            1,
            null,
            1724000000L,
            0,
            "updateLookup");

    ChangeStreamRestriction restriction = fn.getInitialRestriction(partition);
    assertNotNull(restriction);
    assertEquals(0L, restriction.getOffset());
    assertNull(restriction.getResumeTokenJson());

    ChangeStreamRestrictionTracker tracker = fn.newTracker(partition, restriction);
    assertNotNull(tracker);
    assertEquals(restriction, tracker.currentRestriction());

    assertNotNull(fn.getRestrictionCoder());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProcessChangeStreamPartitionFn_burstExecution_zeroDelayContinuation() {
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    MongoCollection<Document> mockCol = mock(MongoCollection.class);
    ChangeStreamIterable<Document> mockStream = mock(ChangeStreamIterable.class);
    MongoChangeStreamCursor<ChangeStreamDocument<Document>> mockCursor = mock(MongoChangeStreamCursor.class);

    when(mockClient.getDatabase(anyString())).thenReturn(mockDb);
    when(mockDb.getCollection(anyString())).thenReturn(mockCol);
    when(mockCol.watch(anyList())).thenReturn(mockStream);
    when(mockStream.batchSize(anyInt())).thenReturn(mockStream);
    when(mockStream.maxAwaitTime(anyLong(), any())).thenReturn(mockStream);
    when(mockStream.fullDocument(any())).thenReturn(mockStream);
    when(mockStream.cursor()).thenReturn(mockCursor);

    Document doc = new Document("_id", 1).append("name", "Alice");
    BsonDocument docKey = new BsonDocument("_id", new BsonInt32(1));
    BsonTimestamp ts = new BsonTimestamp(1724000000, 1);
    ChangeStreamDocument<Document> event = createEvent(OperationType.INSERT, doc, docKey, ts);

    when(mockCursor.tryNext()).thenReturn(event, (ChangeStreamDocument<Document>) null);

    ProcessChangeStreamPartitionFn fn = new ProcessChangeStreamPartitionFn(uri -> mockClient);
    ChangeStreamPartition partition =
        new ChangeStreamPartition(
            "mongodb://localhost:27017",
            "testDb",
            "users",
            "users",
            0,
            1,
            null,
            0,
            0,
            "updateLookup");

    ChangeStreamRestrictionTracker tracker = new ChangeStreamRestrictionTracker(new ChangeStreamRestriction(0L, null));
    OutputReceiver<DocumentWithMetadata> mockReceiver = mock(OutputReceiver.class);

    ProcessContinuation continuation = fn.processElement(partition, tracker, mockReceiver);

    assertTrue(continuation.shouldResume());
    assertEquals(0L, continuation.resumeDelay().getMillis()); // Zero delay for burst
    fn.teardown();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProcessChangeStreamPartitionFn_idleExecution_returnsResumeDelay() {
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    MongoCollection<Document> mockCol = mock(MongoCollection.class);
    ChangeStreamIterable<Document> mockStream = mock(ChangeStreamIterable.class);
    MongoChangeStreamCursor<ChangeStreamDocument<Document>> mockCursor = mock(MongoChangeStreamCursor.class);

    when(mockClient.getDatabase(anyString())).thenReturn(mockDb);
    when(mockDb.getCollection(anyString())).thenReturn(mockCol);
    when(mockCol.watch(anyList())).thenReturn(mockStream);
    when(mockStream.batchSize(anyInt())).thenReturn(mockStream);
    when(mockStream.maxAwaitTime(anyLong(), any())).thenReturn(mockStream);
    when(mockStream.fullDocument(any())).thenReturn(mockStream);
    when(mockStream.cursor()).thenReturn(mockCursor);

    // Idle cursor returns null on tryNext
    when(mockCursor.tryNext()).thenReturn(null);
    // 0x66D57F9B = 1725267867 (Sep 2, 2024)
    when(mockCursor.getResumeToken()).thenReturn(
        new BsonDocument("_data", new BsonString("8266D57F9B000000012B022C0100296E5A1004")));

    ProcessChangeStreamPartitionFn fn = new ProcessChangeStreamPartitionFn(uri -> mockClient);
    ChangeStreamPartition partition =
        new ChangeStreamPartition(
            "mongodb://localhost:27017",
            "testDb",
            "users",
            "users",
            0,
            1,
            null,
            0,
            0,
            "updateLookup");

    ChangeStreamRestrictionTracker tracker = new ChangeStreamRestrictionTracker(new ChangeStreamRestriction(0L, null));
    OutputReceiver<DocumentWithMetadata> mockReceiver = mock(OutputReceiver.class);

    ProcessContinuation continuation = fn.processElement(partition, tracker, mockReceiver);

    assertTrue(continuation.shouldResume());
    assertEquals(200L, continuation.resumeDelay().getMillis()); // 200ms delay for idle
    assertNotNull(tracker.currentRestriction().getResumeTokenJson());
    assertTrue(tracker.currentRestriction().getResumeTokenJson().contains("8266D57F9B"));
    fn.teardown();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProcessChangeStreamPartitionFn_multiPartitionCursorCaching_reusesCursor() {
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    MongoCollection<Document> mockCol = mock(MongoCollection.class);
    ChangeStreamIterable<Document> mockStream = mock(ChangeStreamIterable.class);
    MongoChangeStreamCursor<ChangeStreamDocument<Document>> mockCursor0 = mock(MongoChangeStreamCursor.class);
    MongoChangeStreamCursor<ChangeStreamDocument<Document>> mockCursor1 = mock(MongoChangeStreamCursor.class);

    when(mockClient.getDatabase(anyString())).thenReturn(mockDb);
    when(mockDb.getCollection(anyString())).thenReturn(mockCol);
    when(mockCol.watch(anyList())).thenReturn(mockStream);
    when(mockStream.batchSize(anyInt())).thenReturn(mockStream);
    when(mockStream.maxAwaitTime(anyLong(), any())).thenReturn(mockStream);
    when(mockStream.fullDocument(any())).thenReturn(mockStream);
    when(mockStream.cursor()).thenReturn(mockCursor0, mockCursor1);

    when(mockCursor0.tryNext()).thenReturn(null);
    when(mockCursor1.tryNext()).thenReturn(null);

    ProcessChangeStreamPartitionFn fn = new ProcessChangeStreamPartitionFn(uri -> mockClient);
    ChangeStreamPartition partition0 =
        new ChangeStreamPartition("mongodb://localhost:27017", "testDb", "users", "users", 0, 2, null, 0, 0, "updateLookup");
    ChangeStreamPartition partition1 =
        new ChangeStreamPartition("mongodb://localhost:27017", "testDb", "users", "users", 1, 2, null, 0, 0, "updateLookup");

    OutputReceiver<DocumentWithMetadata> mockReceiver = mock(OutputReceiver.class);

    // Slice 1: Partition 0 (creates cursor0)
    ChangeStreamRestrictionTracker tracker0 = new ChangeStreamRestrictionTracker(new ChangeStreamRestriction(0L, null));
    fn.processElement(partition0, tracker0, mockReceiver);

    // Slice 2: Partition 1 (creates cursor1)
    ChangeStreamRestrictionTracker tracker1 = new ChangeStreamRestrictionTracker(new ChangeStreamRestriction(0L, null));
    fn.processElement(partition1, tracker1, mockReceiver);

    // Slice 3: Partition 0 AGAIN (should REUSE cursor0 without recreating cursor)
    fn.processElement(partition0, tracker0, mockReceiver);

    // Verify stream.cursor() was called exactly 2 times (once per partition), NOT 3 times
    verify(mockStream, times(2)).cursor();

    fn.teardown();
    verify(mockCursor0, times(1)).close();
    verify(mockCursor1, times(1)).close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProcessChangeStreamPartitionFn_transientError_reconnects() {
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    MongoCollection<Document> mockCol = mock(MongoCollection.class);
    ChangeStreamIterable<Document> mockStream = mock(ChangeStreamIterable.class);
    MongoChangeStreamCursor<ChangeStreamDocument<Document>> mockCursor = mock(MongoChangeStreamCursor.class);

    when(mockClient.getDatabase(anyString())).thenReturn(mockDb);
    when(mockDb.getCollection(anyString())).thenReturn(mockCol);
    when(mockCol.watch(anyList())).thenReturn(mockStream);
    when(mockStream.batchSize(anyInt())).thenReturn(mockStream);
    when(mockStream.maxAwaitTime(anyLong(), any())).thenReturn(mockStream);
    when(mockStream.fullDocument(any())).thenReturn(mockStream);
    when(mockStream.cursor()).thenReturn(mockCursor);

    when(mockCursor.tryNext()).thenThrow(new RuntimeException("Socket timeout"));

    ProcessChangeStreamPartitionFn fn = new ProcessChangeStreamPartitionFn(uri -> mockClient);
    ChangeStreamPartition partition =
        new ChangeStreamPartition(
            "mongodb://localhost:27017",
            "testDb",
            "users",
            "users",
            0,
            1,
            null,
            0,
            0,
            "updateLookup");

    ChangeStreamRestrictionTracker tracker = new ChangeStreamRestrictionTracker(new ChangeStreamRestriction(0L, null));
    OutputReceiver<DocumentWithMetadata> mockReceiver = mock(OutputReceiver.class);

    ProcessContinuation continuation = fn.processElement(partition, tracker, mockReceiver);

    assertTrue(continuation.shouldResume());
    assertEquals(1000L, continuation.resumeDelay().getMillis()); // 1000ms delay on error
    fn.teardown();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProcessChangeStreamPartitionFn_wrappedInRestrictionTrackerObserver_doesNotThrowClassCastException() {
    // Simulates Beam's RestrictionTrackerObserver wrapping the delegate RestrictionTracker
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    MongoCollection<Document> mockCol = mock(MongoCollection.class);
    ChangeStreamIterable<Document> mockStream = mock(ChangeStreamIterable.class);
    MongoChangeStreamCursor<ChangeStreamDocument<Document>> mockCursor = mock(MongoChangeStreamCursor.class);

    when(mockClient.getDatabase(anyString())).thenReturn(mockDb);
    when(mockDb.getCollection(anyString())).thenReturn(mockCol);
    when(mockCol.watch(anyList())).thenReturn(mockStream);
    when(mockStream.batchSize(anyInt())).thenReturn(mockStream);
    when(mockStream.maxAwaitTime(anyLong(), any())).thenReturn(mockStream);
    when(mockStream.fullDocument(any())).thenReturn(mockStream);
    when(mockStream.cursor()).thenReturn(mockCursor);

    when(mockCursor.tryNext()).thenReturn(null);

    ProcessChangeStreamPartitionFn fn = new ProcessChangeStreamPartitionFn(uri -> mockClient);
    ChangeStreamPartition partition =
        new ChangeStreamPartition(
            "mongodb://localhost:27017",
            "testDb",
            "users",
            "users",
            0,
            1,
            null,
            0,
            0,
            "updateLookup");

    ChangeStreamRestrictionTracker delegate = new ChangeStreamRestrictionTracker(new ChangeStreamRestriction(0L, null));
    RestrictionTracker<ChangeStreamRestriction, ChangeStreamRestriction> observerWrapper =
        new RestrictionTracker<ChangeStreamRestriction, ChangeStreamRestriction>() {
          @Override
          public boolean tryClaim(ChangeStreamRestriction position) {
            return delegate.tryClaim(position);
          }

          @Override
          public ChangeStreamRestriction currentRestriction() {
            return delegate.currentRestriction();
          }

          @Override
          public SplitResult<ChangeStreamRestriction> trySplit(double fractionOfRemainder) {
            return delegate.trySplit(fractionOfRemainder);
          }

          @Override
          public void checkDone() throws IllegalStateException {
            delegate.checkDone();
          }

          @Override
          public IsBounded isBounded() {
            return delegate.isBounded();
          }
        };

    OutputReceiver<DocumentWithMetadata> mockReceiver = mock(OutputReceiver.class);

    // This must execute cleanly without ClassCastException
    ProcessContinuation continuation = fn.processElement(partition, observerWrapper, mockReceiver);
    assertNotNull(continuation);
    assertTrue(continuation.shouldResume());
    fn.teardown();
  }
}
