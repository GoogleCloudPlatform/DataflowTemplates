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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ReadSplitGenerator}. */
@RunWith(JUnit4.class)
public class ReadSplitGeneratorTest {

  @Test
  public void testGenerateIndexSliceFilters_singleSplit() {
    List<BsonDocument> filters = ReadSplitGenerator.generateIndexSliceFilters(1);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0).isEmpty());
  }

  @Test
  public void testGenerateIndexSliceFilters_zeroSplit() {
    List<BsonDocument> filters = ReadSplitGenerator.generateIndexSliceFilters(0);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0).isEmpty());
  }

  @Test
  public void testGenerateIndexSliceFilters_multipleSplits() {
    List<BsonDocument> filters = ReadSplitGenerator.generateIndexSliceFilters(16);
    assertNotNull(filters);
    assertFalse(filters.isEmpty());
    assertEquals(16, filters.size());

    int numberModCount = 0;
    int stringCount = 0;
    int objectIdCount = 0;
    int catchAllCount = 0;

    for (BsonDocument filter : filters) {
      assertNotNull(filter);
      String json = filter.toJson();
      if (json.contains("\"$not\"")) {
        catchAllCount++;
      }
      if (json.contains("\"$mod\"")) {
        numberModCount++;
      }
      if (json.contains("\"$type\": \"string\"")) {
        stringCount++;
      }
      if (json.contains("\"$oid\"")) {
        objectIdCount++;
      }
    }

    assertEquals(16, numberModCount);
    assertEquals(16, stringCount);
    assertEquals(16, objectIdCount);
    assertEquals(1, catchAllCount);
  }

  @Test
  public void testGenerateIndexSliceFilters_stringOnly_noOrWrapper() {
    List<BsonDocument> filters =
        ReadSplitGenerator.generateIndexSliceFilters(
            4, EnumSet.of(ReadSplitGenerator.IdType.STRING));
    assertEquals(4, filters.size());
    for (BsonDocument filter : filters) {
      String json = filter.toJson();
      assertFalse("Single type filter should not contain $or", json.contains("\"$or\""));
      assertTrue("Should contain string type check", json.contains("\"$type\": \"string\""));
    }
  }

  @Test
  public void testGenerateIndexSliceFilters_objectIdOnly_noOrWrapper() {
    List<BsonDocument> filters =
        ReadSplitGenerator.generateIndexSliceFilters(
            4, EnumSet.of(ReadSplitGenerator.IdType.OBJECT_ID));
    assertEquals(4, filters.size());
    for (BsonDocument filter : filters) {
      String json = filter.toJson();
      assertFalse("Single type filter should not contain $or", json.contains("\"$or\""));
      assertTrue("Should contain $oid check", json.contains("\"$oid\""));
    }
  }

  @Test
  public void testGenerateIndexSliceFilters_numberOnly_noOrWrapper() {
    List<BsonDocument> filters =
        ReadSplitGenerator.generateIndexSliceFilters(
            4, EnumSet.of(ReadSplitGenerator.IdType.NUMBER));
    assertEquals(4, filters.size());
    for (BsonDocument filter : filters) {
      String json = filter.toJson();
      assertFalse("Single type filter should not contain $or", json.contains("\"$or\""));
      assertTrue("Should contain $mod check", json.contains("\"$mod\""));
    }
  }

  @Test
  public void testGenerateIndexSliceFilters_multipleTypes_usesOrWrapper() {
    List<BsonDocument> filters =
        ReadSplitGenerator.generateIndexSliceFilters(
            4, EnumSet.of(ReadSplitGenerator.IdType.STRING, ReadSplitGenerator.IdType.OBJECT_ID));
    assertEquals(4, filters.size());
    for (BsonDocument filter : filters) {
      String json = filter.toJson();
      assertTrue("Multiple type filter should contain $or", json.contains("\"$or\""));
    }
  }

  @Test
  public void testGenerateIndexSliceFilters_otherType_includedInSliceZeroOnly() {
    List<BsonDocument> filters =
        ReadSplitGenerator.generateIndexSliceFilters(
            4, EnumSet.of(ReadSplitGenerator.IdType.STRING, ReadSplitGenerator.IdType.OTHER));
    assertEquals(4, filters.size());
    assertTrue(filters.get(0).toJson().contains("\"$not\""));
    assertFalse(filters.get(1).toJson().contains("\"$not\""));
    assertFalse(filters.get(2).toJson().contains("\"$not\""));
    assertFalse(filters.get(3).toJson().contains("\"$not\""));
  }

  @Test
  public void testDataDrivenSplits_mixedTypesAreIsolated() {
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);

    when(mockClient.getDatabase(org.mockito.ArgumentMatchers.anyString())).thenReturn(mockDb);
    when(mockDb.getCollection(
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.eq(BsonDocument.class)))
        .thenReturn(mockCol);

    // Mock detectIdTypes to return multiple types
    @SuppressWarnings("unchecked")
    FindIterable<BsonDocument> mockFind = mock(FindIterable.class);
    when(mockCol.find(any(BsonDocument.class))).thenReturn(mockFind);
    when(mockFind.limit(1)).thenReturn(mockFind);
    when(mockFind.first()).thenReturn(new BsonDocument()); // Meaning we detect active types

    // Mock $sample aggregation
    @SuppressWarnings("unchecked")
    com.mongodb.client.AggregateIterable<BsonDocument> mockAgg =
        (com.mongodb.client.AggregateIterable<BsonDocument>)
            java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {com.mongodb.client.AggregateIterable.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("allowDiskUse")
                      || method.getName().equals("maxTime")) {
                    return proxy;
                  }
                  if (method.getName().equals("iterator")) {
                    return new com.mongodb.client.MongoCursor<BsonDocument>() {
                      java.util.Iterator<BsonDocument> iter =
                          java.util.Arrays.asList(
                                  new BsonDocument("_id", new org.bson.BsonString("min")),
                                  new BsonDocument("_id", new org.bson.BsonString("mid")),
                                  new BsonDocument("_id", new org.bson.BsonString("max")))
                              .iterator();

                      @Override
                      public void close() {}

                      @Override
                      public boolean hasNext() {
                        return iter.hasNext();
                      }

                      @Override
                      public BsonDocument next() {
                        return iter.next();
                      }

                      @Override
                      public BsonDocument tryNext() {
                        return null;
                      }

                      @Override
                      public com.mongodb.ServerCursor getServerCursor() {
                        return null;
                      }

                      @Override
                      public com.mongodb.ServerAddress getServerAddress() {
                        return null;
                      }

                      @Override
                      public int available() {
                        return 0;
                      }
                    };
                  }
                  return null;
                });
    when(mockCol.aggregate(any())).thenReturn(mockAgg);

    List<BsonDocument> filters =
        ReadSplitGenerator.generateIndexSliceFilters(mockClient, "db", "col", 2);

    assertEquals(2, filters.size());
    String slice0 = filters.get(0).toJson();
    String slice1 = filters.get(1).toJson();

    // Validate we use the $or wrapper
    assertTrue(slice0.contains("\"$or\""));

    // Validate that the bounds are nested within specific type bounds!
    assertTrue(slice0.contains("\"$type\": \"string\""));
    assertTrue(slice0.contains("\"$type\": \"objectId\""));
    assertTrue(slice0.contains("\"$type\": [\"int\""));
  }

  @Test
  public void testGenerateTypeIsolatedSplits_smallCollectionReturnsSingleSplit() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    when(mockCol.estimatedDocumentCount()).thenReturn(2000L);

    List<BsonDocument> splits = ReadSplitGenerator.generateTypeIsolatedSplits(mockCol, 16);
    assertEquals(1, splits.size());
    assertTrue(splits.get(0).isEmpty());
  }

  @Test
  public void testGenerateTypeIsolatedSplits_adaptiveVolumeSizing_calculatesEffectiveSplits() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    when(mockCol.estimatedDocumentCount()).thenReturn(1_000_000L);

    List<BsonDocument> splits =
        ReadSplitGenerator.generateTypeIsolatedSplits(mockCol, 1, 200_000, 256);
    assertEquals(5, splits.size());
  }

  @Test
  public void testGenerateTypeIsolatedSplits_adaptiveVolumeSizing_clampsToMaxSplits() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    when(mockCol.estimatedDocumentCount()).thenReturn(100_000_000L);

    List<BsonDocument> splits =
        ReadSplitGenerator.generateTypeIsolatedSplits(mockCol, 1, 200_000, 256);
    assertEquals(256, splits.size());
  }

  @Test
  public void testGenerateTypeIsolatedSplits_zeroTargetChunkSize_usesNumSplits() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    when(mockCol.estimatedDocumentCount()).thenReturn(10_000_000L);

    List<BsonDocument> splits = ReadSplitGenerator.generateTypeIsolatedSplits(mockCol, 16, 0, 256);
    assertEquals(16, splits.size());
  }

  @Test
  public void testGenerateProbedObjectIdSplits_leavesTailSliceUnboundedAbove() {
    String minHex = "66d000000000000000000000";
    String maxHex = "66dff0000000000000000000";
    List<BsonDocument> splits = ReadSplitGenerator.generateProbedObjectIdSplits(minHex, maxHex, 4);
    assertEquals(4, splits.size());
    assertTrue(splits.get(0).toJson().contains("\"$lt\""));
    assertTrue(splits.get(1).toJson().contains("\"$gte\""));
    assertTrue(splits.get(1).toJson().contains("\"$lt\""));
    String tailJson = splits.get(3).toJson();
    assertTrue(tailJson.contains("\"$gte\""));
    assertFalse(tailJson.contains("\"$lte\""));
  }

  @Test
  public void testGenerateProbedObjectIdSplits_singleSplitUnboundedAbove() {
    String minHex = "66d000000000000000000000";
    String maxHex = "66dff0000000000000000000";
    List<BsonDocument> splits = ReadSplitGenerator.generateProbedObjectIdSplits(minHex, maxHex, 1);
    assertEquals(1, splits.size());
    String json = splits.get(0).toJson();
    assertTrue(json.contains("\"$type\": \"objectId\""));
    assertFalse(json.contains("\"$lte\""));
  }

  @Test
  public void testGetBucketForValue_identifiesAllBsonTypes() {
    assertEquals(
        "number", ReadSplitGenerator.getBucketForValue(new org.bson.BsonInt32(42)).getName());
    assertEquals(
        "number", ReadSplitGenerator.getBucketForValue(new org.bson.BsonInt64(42L)).getName());
    assertEquals(
        "number", ReadSplitGenerator.getBucketForValue(new org.bson.BsonDouble(42.5)).getName());
    assertEquals(
        "string", ReadSplitGenerator.getBucketForValue(new org.bson.BsonString("test")).getName());
    assertEquals(
        "objectId",
        ReadSplitGenerator.getBucketForValue(
                new org.bson.BsonObjectId(new org.bson.types.ObjectId("600000000000000000000001")))
            .getName());
    assertEquals(
        "bool", ReadSplitGenerator.getBucketForValue(new org.bson.BsonBoolean(true)).getName());
    assertEquals(
        "date",
        ReadSplitGenerator.getBucketForValue(new org.bson.BsonDateTime(1700000000000L)).getName());
    assertEquals(
        "binData",
        ReadSplitGenerator.getBucketForValue(new org.bson.BsonBinary(new byte[] {1, 2, 3}))
            .getName());
    assertEquals(
        "object",
        ReadSplitGenerator.getBucketForValue(new BsonDocument("sub", new org.bson.BsonInt32(1)))
            .getName());
    assertEquals("null", ReadSplitGenerator.getBucketForValue(org.bson.BsonNull.VALUE).getName());
    assertEquals(
        "timestamp",
        ReadSplitGenerator.getBucketForValue(new org.bson.BsonTimestamp(100, 1)).getName());
  }

  @Test
  public void testProbeActiveTypeBounds_homogeneous_singleTypeImmediateReturn() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    @SuppressWarnings("unchecked")
    FindIterable<BsonDocument> mockFind = mock(FindIterable.class);

    when(mockCol.find()).thenReturn(mockFind);
    when(mockFind.projection(any())).thenReturn(mockFind);
    when(mockFind.sort(any())).thenReturn(mockFind);
    when(mockFind.limit(any(Integer.class))).thenReturn(mockFind);
    when(mockFind.maxTime(any(Long.class), any())).thenReturn(mockFind);

    org.bson.BsonObjectId minOid =
        new org.bson.BsonObjectId(new org.bson.types.ObjectId("600000000000000000000001"));
    org.bson.BsonObjectId maxOid =
        new org.bson.BsonObjectId(new org.bson.types.ObjectId("600000000000000000000009"));

    // First call is global min sort({_id: 1}), second call is global max sort({_id: -1})
    when(mockFind.first())
        .thenReturn(new BsonDocument("_id", minOid))
        .thenReturn(new BsonDocument("_id", maxOid));

    List<ReadSplitGenerator.ProbedTypeBounds> bounds =
        ReadSplitGenerator.probeActiveTypeBounds(mockCol);

    assertEquals(1, bounds.size());
    ReadSplitGenerator.ProbedTypeBounds b = bounds.get(0);
    assertEquals("objectId", b.getBucket().getName());
    assertEquals(minOid, b.getMinKey());
    assertEquals(maxOid, b.getMaxKey());
  }

  @Test
  public void testProbeActiveTypeBounds_mixedCollection_discoversAllActiveTypes() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    @SuppressWarnings("unchecked")
    FindIterable<BsonDocument> mockGlobalFind = mock(FindIterable.class);
    @SuppressWarnings("unchecked")
    FindIterable<BsonDocument> mockBucketFind = mock(FindIterable.class);

    when(mockCol.find()).thenReturn(mockGlobalFind);
    when(mockGlobalFind.projection(any())).thenReturn(mockGlobalFind);
    when(mockGlobalFind.sort(any())).thenReturn(mockGlobalFind);
    when(mockGlobalFind.limit(any(Integer.class))).thenReturn(mockGlobalFind);
    when(mockGlobalFind.maxTime(any(Long.class), any())).thenReturn(mockGlobalFind);

    when(mockCol.find(any(BsonDocument.class))).thenReturn(mockBucketFind);
    when(mockBucketFind.projection(any())).thenReturn(mockBucketFind);
    when(mockBucketFind.sort(any())).thenReturn(mockBucketFind);
    when(mockBucketFind.limit(any(Integer.class))).thenReturn(mockBucketFind);
    when(mockBucketFind.maxTime(any(Long.class), any())).thenReturn(mockBucketFind);

    org.bson.BsonInt32 minNum = new org.bson.BsonInt32(1);
    org.bson.BsonInt32 maxNum = new org.bson.BsonInt32(500);
    org.bson.BsonString minStr = new org.bson.BsonString("a");
    org.bson.BsonString maxStr = new org.bson.BsonString("z");
    org.bson.BsonObjectId minOid =
        new org.bson.BsonObjectId(new org.bson.types.ObjectId("600000000000000000000001"));
    org.bson.BsonObjectId maxOid =
        new org.bson.BsonObjectId(new org.bson.types.ObjectId("600000000000000000000009"));

    // Global min = number(1), Global max = objectId(maxOid)
    when(mockGlobalFind.first())
        .thenReturn(new BsonDocument("_id", minNum))
        .thenReturn(new BsonDocument("_id", maxOid));

    // Bucket probes in canonical order between number(idx 2) and objectId(idx 7):
    // 1. minBucket(number) max -> maxNum
    // 2. candidate(string) min -> minStr, max -> maxStr
    // 3. candidate(symbol) min -> null
    // 4. candidate(object) min -> null
    // 5. candidate(binData) min -> null
    // 6. maxBucket(objectId) min -> minOid
    when(mockBucketFind.first())
        .thenReturn(new BsonDocument("_id", maxNum))
        .thenReturn(new BsonDocument("_id", minStr))
        .thenReturn(new BsonDocument("_id", maxStr))
        .thenReturn(null)
        .thenReturn(null)
        .thenReturn(null)
        .thenReturn(new BsonDocument("_id", minOid));

    List<ReadSplitGenerator.ProbedTypeBounds> bounds =
        ReadSplitGenerator.probeActiveTypeBounds(mockCol);

    assertEquals(3, bounds.size());
    assertEquals("number", bounds.get(0).getBucket().getName());
    assertEquals(minNum, bounds.get(0).getMinKey());
    assertEquals(maxNum, bounds.get(0).getMaxKey());
    assertEquals("string", bounds.get(1).getBucket().getName());
    assertEquals(minStr, bounds.get(1).getMinKey());
    assertEquals(maxStr, bounds.get(1).getMaxKey());
    assertEquals("objectId", bounds.get(2).getBucket().getName());
    assertEquals(minOid, bounds.get(2).getMinKey());
    assertEquals(maxOid, bounds.get(2).getMaxKey());
  }

  @Test
  public void testProbeActiveTypeBounds_intermediateTimeout_abortsWithoutSilentDrop() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    @SuppressWarnings("unchecked")
    FindIterable<BsonDocument> mockGlobalFind = mock(FindIterable.class);
    @SuppressWarnings("unchecked")
    FindIterable<BsonDocument> mockBucketFind = mock(FindIterable.class);

    when(mockCol.find()).thenReturn(mockGlobalFind);
    when(mockGlobalFind.projection(any())).thenReturn(mockGlobalFind);
    when(mockGlobalFind.sort(any())).thenReturn(mockGlobalFind);
    when(mockGlobalFind.limit(any(Integer.class))).thenReturn(mockGlobalFind);
    when(mockGlobalFind.maxTime(any(Long.class), any())).thenReturn(mockGlobalFind);

    when(mockCol.find(any(BsonDocument.class))).thenReturn(mockBucketFind);
    when(mockBucketFind.projection(any())).thenReturn(mockBucketFind);
    when(mockBucketFind.sort(any())).thenReturn(mockBucketFind);
    when(mockBucketFind.limit(any(Integer.class))).thenReturn(mockBucketFind);
    when(mockBucketFind.maxTime(any(Long.class), any())).thenReturn(mockBucketFind);

    when(mockGlobalFind.first())
        .thenReturn(new BsonDocument("_id", new org.bson.BsonInt32(1)))
        .thenReturn(
            new BsonDocument(
                "_id",
                new org.bson.BsonObjectId(
                    new org.bson.types.ObjectId("600000000000000000000009"))));

    // Simulate timeout exception on intermediate bucket probe
    when(mockBucketFind.first())
        .thenReturn(new BsonDocument("_id", new org.bson.BsonInt32(100)))
        .thenThrow(new RuntimeException("MongoExecutionTimeoutException"));

    List<ReadSplitGenerator.ProbedTypeBounds> bounds =
        ReadSplitGenerator.probeActiveTypeBounds(mockCol);

    // Must abort and return empty list rather than silently dropping the timed-out type!
    assertTrue(bounds.isEmpty());
  }

  @Test
  public void testGenerateProbedNumberSplits_handlesNegativeNumbersAndLarge64BitLongs() {
    List<BsonDocument> splits =
        ReadSplitGenerator.generateProbedNumberSplits(
            new org.bson.BsonInt64(-1000L), new org.bson.BsonInt64(1000L), 4);
    assertEquals(4, splits.size());
    assertTrue(splits.get(0).toJson().contains("\"$lt\""));
    assertFalse(splits.get(0).toJson().contains("\"$gte\""));
    assertTrue(splits.get(3).toJson().contains("\"$gte\""));
    assertFalse(splits.get(3).toJson().contains("\"$lt\""));
  }

  @Test
  public void testProbeActiveTypeBounds_emptyCollectionReturnsEmptyList() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    @SuppressWarnings("unchecked")
    FindIterable<BsonDocument> mockFind = mock(FindIterable.class);

    when(mockCol.find()).thenReturn(mockFind);
    when(mockFind.projection(any())).thenReturn(mockFind);
    when(mockFind.sort(any())).thenReturn(mockFind);
    when(mockFind.limit(any(Integer.class))).thenReturn(mockFind);
    when(mockFind.maxTime(any(Long.class), any())).thenReturn(mockFind);
    when(mockFind.first()).thenReturn(null);

    List<ReadSplitGenerator.ProbedTypeBounds> bounds =
        ReadSplitGenerator.probeActiveTypeBounds(mockCol);

    assertTrue(bounds.isEmpty());
  }

  @Test
  public void testGenerateProbedNumberSplits_preservesDoubleAndDecimal128Precision() {
    List<BsonDocument> doubleSplits =
        ReadSplitGenerator.generateProbedNumberSplits(
            new org.bson.BsonDouble(0.0), new org.bson.BsonDouble(1.0), 4);
    assertEquals(4, doubleSplits.size());
    assertTrue(doubleSplits.get(0).toJson().contains("0.25"));
    assertTrue(doubleSplits.get(1).toJson().contains("0.25"));
    assertTrue(doubleSplits.get(1).toJson().contains("0.5"));
    assertTrue(doubleSplits.get(3).toJson().contains("0.75"));

    List<BsonDocument> decSplits =
        ReadSplitGenerator.generateProbedNumberSplits(
            new org.bson.BsonDecimal128(
                new org.bson.types.Decimal128(new java.math.BigDecimal("0.0001"))),
            new org.bson.BsonDecimal128(
                new org.bson.types.Decimal128(new java.math.BigDecimal("0.0005"))),
            4);
    assertEquals(4, decSplits.size());
    assertTrue(decSplits.get(0).toJson().contains("0.0002"));
    assertTrue(decSplits.get(3).toJson().contains("0.0004"));

    // Degenerate range (min >= max or numSplits <= 1) returns single unsplit number filter
    List<BsonDocument> singleSplit =
        ReadSplitGenerator.generateProbedNumberSplits(
            new org.bson.BsonDouble(5.0), new org.bson.BsonDouble(5.0), 4);
    assertEquals(1, singleSplit.size());
  }

  @Test
  public void testGenerateFilterStrings_allCanonicalBsonTypesSupported() {
    List<BsonValue> sampleDocs =
        Arrays.asList(
            new org.bson.BsonMinKey(),
            new org.bson.BsonNull(),
            new org.bson.BsonInt32(10),
            new org.bson.BsonString("abc"),
            new org.bson.BsonSymbol("sym"),
            new BsonDocument("k", new org.bson.BsonInt32(1)),
            new org.bson.BsonBinary(new byte[] {1, 2}),
            new org.bson.BsonObjectId(new org.bson.types.ObjectId("600000000000000000000001")),
            new org.bson.BsonBoolean(false),
            new org.bson.BsonDateTime(1000L),
            new org.bson.BsonTimestamp(100, 1),
            new org.bson.BsonRegularExpression("^a"),
            new org.bson.BsonMaxKey());

    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    @SuppressWarnings("unchecked")
    FindIterable<BsonDocument> mockGlobalFind = mock(FindIterable.class);
    @SuppressWarnings("unchecked")
    FindIterable<BsonDocument> mockBucketFind = mock(FindIterable.class);

    when(mockCol.find()).thenReturn(mockGlobalFind);
    when(mockGlobalFind.projection(any())).thenReturn(mockGlobalFind);
    when(mockGlobalFind.sort(any())).thenReturn(mockGlobalFind);
    when(mockGlobalFind.limit(any(Integer.class))).thenReturn(mockGlobalFind);
    when(mockGlobalFind.maxTime(any(Long.class), any())).thenReturn(mockGlobalFind);
    when(mockGlobalFind.first())
        .thenReturn(new BsonDocument("_id", new org.bson.BsonMinKey()))
        .thenReturn(new BsonDocument("_id", new org.bson.BsonMaxKey()));

    when(mockCol.find(any(org.bson.conversions.Bson.class))).thenReturn(mockBucketFind);
    when(mockBucketFind.projection(any())).thenReturn(mockBucketFind);
    when(mockBucketFind.sort(any())).thenReturn(mockBucketFind);
    when(mockBucketFind.limit(any(Integer.class))).thenReturn(mockBucketFind);
    when(mockBucketFind.maxTime(any(Long.class), any())).thenReturn(mockBucketFind);

    // minBucket (minKey) max is minKey; intermediate buckets each return their sample value for min
    // & max; maxBucket (maxKey) min is maxKey
    when(mockBucketFind.first())
        .thenReturn(new BsonDocument("_id", sampleDocs.get(0))) // minBucketMax
        .thenReturn(new BsonDocument("_id", sampleDocs.get(1))) // null min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(1))) // null max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(2))) // number min
        .thenReturn(new BsonDocument("_id", new org.bson.BsonInt32(20))) // number max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(3))) // string min
        .thenReturn(new BsonDocument("_id", new org.bson.BsonString("xyz"))) // string max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(4))) // symbol min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(4))) // symbol max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(5))) // object min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(5))) // object max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(6))) // binData min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(6))) // binData max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(7))) // objectId min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(7))) // objectId max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(8))) // bool min
        .thenReturn(new BsonDocument("_id", new org.bson.BsonBoolean(true))) // bool max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(9))) // date min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(9))) // date max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(10))) // timestamp min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(10))) // timestamp max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(11))) // regex min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(11))) // regex max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(12))); // maxBucketMin

    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    when(mockClient.getDatabase("db")).thenReturn(mockDb);
    when(mockDb.getCollection("col", BsonDocument.class)).thenReturn(mockCol);
    when(mockDb.runCommand(any())).thenReturn(new org.bson.Document("count", 10000));

    @SuppressWarnings("unchecked")
    AggregateIterable<BsonDocument> mockAgg = mock(AggregateIterable.class);
    @SuppressWarnings("unchecked")
    MongoCursor<BsonDocument> mockCursor = mock(MongoCursor.class);
    when(mockCol.aggregate(any())).thenReturn(mockAgg);
    when(mockAgg.allowDiskUse(any(Boolean.class))).thenReturn(mockAgg);
    when(mockAgg.maxTime(any(Long.class), any())).thenReturn(mockAgg);
    when(mockAgg.iterator()).thenReturn(mockCursor);

    // Feed all sampleDocs through $sample cursor to exercise matchesType for every BSON type
    List<BsonDocument> wrappedDocs = new java.util.ArrayList<>();
    for (BsonValue v : sampleDocs) {
      wrappedDocs.add(new BsonDocument("_id", v));
    }
    java.util.Iterator<BsonDocument> it = wrappedDocs.iterator();
    when(mockCursor.hasNext()).thenAnswer(inv -> it.hasNext());
    when(mockCursor.next()).thenAnswer(inv -> it.next());

    List<BsonDocument> splits =
        ReadSplitGenerator.generateIndexSliceFilters(mockClient, "db", "col", 26);
    assertFalse(splits.isEmpty());
  }
}
