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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.transforms.MongoDbBackfillReader.BackfillPartition;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

/** Unit tests for {@link MongoDbBackfillReader}. */
@RunWith(JUnit4.class)
public class MongoDbBackfillReaderTest {

  @Test
  public void generatePartitions_withoutTimestamp_returnsSingleRootPartition() {
    List<BackfillPartition> partitions =
        MongoDbBackfillReader.generatePartitions(
            "mongodb://localhost:27017", "testDb", "srcCol", "tgtCol", null);

    assertEquals(1, partitions.size());
    BackfillPartition partition = partitions.get(0);
    assertEquals("srcCol", partition.getSourceCollection());
    assertEquals("tgtCol", partition.getTargetCollection());
    assertNull(partition.getFilterJson());
    assertNull(partition.getTimestampSortKey());
    assertEquals(0, partition.getPartitionIndex());
    assertEquals(1, partition.getTotalPartitions());
    assertFalse(partition.hasFilter());
  }

  @Test
  public void generatePartitions_withTimestamp_returnsRootPartitionWithSortKey() {
    BsonTimestamp t0 = new BsonTimestamp(1700000000, 5);
    List<BackfillPartition> partitions =
        MongoDbBackfillReader.generatePartitions(
            "mongodb://localhost:27017", "testDb", "srcCol", "tgtCol", t0);

    assertEquals(1, partitions.size());
    BackfillPartition partition = partitions.get(0);
    assertEquals("srcCol", partition.getSourceCollection());
    assertEquals("tgtCol", partition.getTargetCollection());
    assertNull(partition.getFilterJson());
    assertNotNull(partition.getTimestampSortKey());
    assertEquals(1700000000L, partition.getTimestampSortKey().getSeconds());
    assertFalse(partition.getTimestampSortKey().isCdc());
  }

  @Test
  public void generatePartitions_withNumSplits_returnsCoarsePartitions() {
    BsonTimestamp t0 = new BsonTimestamp(1700000000, 5);
    List<BackfillPartition> partitions =
        MongoDbBackfillReader.generatePartitions(
            "mongodb://localhost:27017", "testDb", "srcCol", "tgtCol", 4, t0);

    assertEquals(4, partitions.size());
    for (int i = 0; i < 4; i++) {
      BackfillPartition p = partitions.get(i);
      assertEquals("srcCol", p.getSourceCollection());
      assertEquals("tgtCol", p.getTargetCollection());
      assertEquals(i, p.getPartitionIndex());
      assertEquals(4, p.getTotalPartitions());
      assertNotNull(p.getFilterJson());
      assertEquals(1700000000L, p.getTimestampSortKey().getSeconds());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void generatePartitions_withMongoClient_returnsDataDrivenPartitions() {
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    FindIterable<BsonDocument> mockFindIterable = mock(FindIterable.class);

    when(mockClient.getDatabase(anyString())).thenReturn(mockDb);
    when(mockDb.getCollection(anyString(), org.mockito.ArgumentMatchers.eq(BsonDocument.class)))
        .thenReturn(mockCol);
    when(mockCol.find(any(BsonDocument.class))).thenReturn(mockFindIterable);
    when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);
    when(mockFindIterable.first()).thenReturn(null);

    BsonTimestamp t0 = new BsonTimestamp(1700000000, 5);
    List<BackfillPartition> partitions =
        MongoDbBackfillReader.generatePartitions(
            mockClient, "mongodb://localhost:27017", "testDb", "srcCol", "tgtCol", 2, t0);

    assertEquals(2, partitions.size());
    assertEquals(0, partitions.get(0).getPartitionIndex());
    assertEquals(1, partitions.get(1).getPartitionIndex());
  }

  @Test
  public void backfillPartition_equalsAndHashCode() {
    BackfillPartition p1 =
        new BackfillPartition("uri", "db", "src", "tgt", "{}", TimestampSortKey.backfill(100), 0, 2);
    BackfillPartition p2 =
        new BackfillPartition("uri", "db", "src", "tgt", "{}", TimestampSortKey.backfill(100), 0, 2);
    BackfillPartition p3 =
        new BackfillPartition("uri", "db", "src", "tgt", "{}", TimestampSortKey.backfill(100), 1, 2);

    assertEquals(p1, p2);
    assertEquals(p1.hashCode(), p2.hashCode());
    assertFalse(p1.equals(p3));
  }

  @Test
  public void backfillPartition_toString_containsDetails() {
    BackfillPartition p =
        new BackfillPartition("uri", "db", "src", "tgt", "{}", TimestampSortKey.backfill(100), 0, 2);
    String str = p.toString();
    assertTrue(str.contains("src"));
    assertTrue(str.contains("0/2"));
  }

  @Test
  public void backfillRestriction_propertiesAndLastSeenId() {
    MongoDbBackfillReader.BackfillRestriction r1 =
        new MongoDbBackfillReader.BackfillRestriction(10L, "{\"_id\": 123}", false);
    MongoDbBackfillReader.BackfillRestriction r2 =
        new MongoDbBackfillReader.BackfillRestriction(10L, "{\"_id\": 123}", false);
    MongoDbBackfillReader.BackfillRestriction r3 =
        new MongoDbBackfillReader.BackfillRestriction(20L, null, true);

    assertEquals(10L, r1.getOffset());
    assertEquals("{\"_id\": 123}", r1.getLastSeenIdJson());
    assertFalse(r1.isDone());
    assertEquals(new BsonInt32(123), r1.getLastSeenId());

    assertEquals(r1, r2);
    assertEquals(r1.hashCode(), r2.hashCode());
    assertFalse(r1.equals(r3));
    assertTrue(r1.toString().contains("offset=10"));
    assertNull(r3.getLastSeenId());
  }

  @Test
  public void backfillRestrictionTracker_tryClaimAndSplit() {
    MongoDbBackfillReader.BackfillRestriction initial =
        new MongoDbBackfillReader.BackfillRestriction(0L, null, false);
    MongoDbBackfillReader.BackfillRestrictionTracker tracker =
        new MongoDbBackfillReader.BackfillRestrictionTracker(initial);

    assertEquals(initial, tracker.currentRestriction());
    assertTrue(tracker.tryClaim(new MongoDbBackfillReader.BackfillRestriction(1L, "{\"_id\": 1}", false)));
    assertEquals(1L, tracker.currentRestriction().getOffset());

    org.apache.beam.sdk.transforms.splittabledofn.SplitResult<MongoDbBackfillReader.BackfillRestriction> split =
        tracker.trySplit(0.0);
    assertNotNull(split);
    assertEquals(1L, split.getPrimary().getOffset());
    assertEquals(1L, split.getResidual().getOffset());

    // After stop, claiming returns false
    assertFalse(tracker.tryClaim(new MongoDbBackfillReader.BackfillRestriction(2L, "{\"_id\": 2}", false)));
    assertEquals(org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.IsBounded.BOUNDED, tracker.isBounded());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void processBackfillPartitionFn_readsAndEmitsDocuments() {
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    MongoCollection<Document> mockCol = mock(MongoCollection.class);
    FindIterable<Document> mockFindIterable = mock(FindIterable.class);
    MongoCursor<Document> mockCursor = mock(MongoCursor.class);

    when(mockClient.getDatabase(anyString())).thenReturn(mockDb);
    when(mockDb.getCollection(anyString())).thenReturn(mockCol);
    when(mockCol.find(any(Bson.class))).thenReturn(mockFindIterable);
    when(mockFindIterable.sort(any(Bson.class))).thenReturn(mockFindIterable);
    when(mockFindIterable.batchSize(anyInt())).thenReturn(mockFindIterable);
    when(mockFindIterable.iterator()).thenReturn(mockCursor);

    Document doc1 = new Document("_id", 1).append("name", "Alice");
    Document doc2 = new Document("_id", 2).append("name", "Bob");

    when(mockCursor.hasNext()).thenReturn(true, true, false);
    when(mockCursor.next()).thenReturn(doc1, doc2);

    TimestampSortKey sortKey = TimestampSortKey.backfill(1700000000L);
    BackfillPartition partition =
        new BackfillPartition(
            "mongodb://localhost:27017",
            "testDb",
            "users",
            "users_target",
            new BsonDocument("_id", new BsonDocument("$gte", new BsonInt32(0))).toJson(),
            sortKey,
            0,
            1);

    MongoDbBackfillReader.ProcessBackfillPartitionFn fn =
        new MongoDbBackfillReader.ProcessBackfillPartitionFn(uri -> mockClient);

    MongoDbBackfillReader.BackfillRestriction restriction =
        fn.getInitialRestriction(partition);
    MongoDbBackfillReader.BackfillRestrictionTracker tracker =
        fn.newTracker(partition, restriction);
    assertNotNull(fn.getRestrictionCoder());

    DoFn.OutputReceiver<DocumentWithMetadata> receiver =
        mock(DoFn.OutputReceiver.class);

    DoFn.ProcessContinuation continuation =
        fn.processElement(partition, tracker, receiver);

    assertEquals(DoFn.ProcessContinuation.stop(), continuation);

    ArgumentCaptor<DocumentWithMetadata> captor =
        ArgumentCaptor.forClass(DocumentWithMetadata.class);
    verify(receiver, org.mockito.Mockito.times(2)).output(captor.capture());

    List<DocumentWithMetadata> results = captor.getAllValues();
    assertEquals(2, results.size());

    assertEquals(Integer.valueOf(1), results.get(0).getDocument().get("_id"));
    assertEquals("users", results.get(0).getSourceCollection());
    assertEquals("users_target", results.get(0).getTargetCollection());
    assertEquals(sortKey, results.get(0).getTimestampSortKey());

    assertEquals(Integer.valueOf(2), results.get(1).getDocument().get("_id"));
    assertEquals("users", results.get(1).getSourceCollection());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void processBackfillPartitionFn_keysetResume_usesLastSeenId() {
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    MongoCollection<Document> mockCol = mock(MongoCollection.class);
    FindIterable<Document> mockFindIterable = mock(FindIterable.class);
    MongoCursor<Document> mockCursor = mock(MongoCursor.class);

    when(mockClient.getDatabase(anyString())).thenReturn(mockDb);
    when(mockDb.getCollection(anyString())).thenReturn(mockCol);
    when(mockCol.find(any(Bson.class))).thenReturn(mockFindIterable);
    when(mockFindIterable.sort(any(Bson.class))).thenReturn(mockFindIterable);
    when(mockFindIterable.batchSize(anyInt())).thenReturn(mockFindIterable);
    when(mockFindIterable.iterator()).thenReturn(mockCursor);

    Document doc3 = new Document("_id", 3).append("name", "Charlie");
    when(mockCursor.hasNext()).thenReturn(true, false);
    when(mockCursor.next()).thenReturn(doc3);

    BackfillPartition partition =
        new BackfillPartition(
            "mongodb://localhost:27017",
            "testDb",
            "users",
            "users_target",
            null,
            TimestampSortKey.backfill(1700000000L),
            0,
            1);

    MongoDbBackfillReader.ProcessBackfillPartitionFn fn =
        new MongoDbBackfillReader.ProcessBackfillPartitionFn(uri -> mockClient);

    MongoDbBackfillReader.BackfillRestriction resumedRestriction =
        new MongoDbBackfillReader.BackfillRestriction(2L, "{\"_id\": 2}", false);
    MongoDbBackfillReader.BackfillRestrictionTracker tracker =
        fn.newTracker(partition, resumedRestriction);

    DoFn.OutputReceiver<DocumentWithMetadata> receiver =
        mock(DoFn.OutputReceiver.class);

    DoFn.ProcessContinuation continuation =
        fn.processElement(partition, tracker, receiver);

    assertEquals(DoFn.ProcessContinuation.stop(), continuation);
    verify(receiver, org.mockito.Mockito.times(1)).output(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void processBackfillPartitionFn_transientException_returnsResumeWithDelay() {
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    MongoCollection<Document> mockCol = mock(MongoCollection.class);

    when(mockClient.getDatabase(anyString())).thenReturn(mockDb);
    when(mockDb.getCollection(anyString())).thenReturn(mockCol);
    when(mockCol.find(any(Bson.class))).thenThrow(new com.mongodb.MongoSocketException("Socket error", new com.mongodb.ServerAddress()));

    BackfillPartition partition =
        new BackfillPartition(
            "mongodb://localhost:27017",
            "testDb",
            "users",
            "users_target",
            null,
            TimestampSortKey.backfill(1700000000L),
            0,
            1);

    MongoDbBackfillReader.ProcessBackfillPartitionFn fn =
        new MongoDbBackfillReader.ProcessBackfillPartitionFn(uri -> mockClient);

    MongoDbBackfillReader.BackfillRestriction restriction =
        fn.getInitialRestriction(partition);
    MongoDbBackfillReader.BackfillRestrictionTracker tracker =
        fn.newTracker(partition, restriction);

    DoFn.OutputReceiver<DocumentWithMetadata> receiver =
        mock(DoFn.OutputReceiver.class);

    DoFn.ProcessContinuation continuation =
        fn.processElement(partition, tracker, receiver);

    assertNotNull(continuation);
    assertTrue(continuation.shouldResume());
  }

  @Test
  public void readPartitions_emptyPartitions_expandsSuccessfully() {
    Pipeline p = Pipeline.create();
    PCollection<DocumentWithMetadata> docs =
        p.apply(
            "ReadEmpty",
            new MongoDbBackfillReader.ReadPartitions(Collections.emptyList()));
    assertNotNull(docs);
  }

  @Test
  public void readPartitions_withPartitions_expandsSuccessfully() {
    Pipeline p = Pipeline.create();
    BackfillPartition partition =
        new BackfillPartition(
            "mongodb://localhost:27017",
            "testDb",
            "users",
            "users_target",
            null,
            TimestampSortKey.backfill(1700000000L),
            0,
            1);
    PCollection<DocumentWithMetadata> docs =
        p.apply(
            "ReadPartitions",
            new MongoDbBackfillReader.ReadPartitions(Collections.singletonList(partition)));
    assertNotNull(docs);
  }
}
