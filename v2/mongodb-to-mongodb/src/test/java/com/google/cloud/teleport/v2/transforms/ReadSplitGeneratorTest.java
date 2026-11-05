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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
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
  public void getBucketForValue_identifiesAllBsonTypes() {
    assertEquals("number", ReadSplitGenerator.getBucketForValue(new BsonInt32(42)).getName());
    assertEquals("number", ReadSplitGenerator.getBucketForValue(new BsonInt64(42L)).getName());
    assertEquals("number", ReadSplitGenerator.getBucketForValue(new BsonDouble(42.5)).getName());
    assertEquals("string", ReadSplitGenerator.getBucketForValue(new BsonString("test")).getName());
    assertEquals(
        "objectId",
        ReadSplitGenerator.getBucketForValue(
                new BsonObjectId(new ObjectId("600000000000000000000001")))
            .getName());
    assertEquals("bool", ReadSplitGenerator.getBucketForValue(new BsonBoolean(true)).getName());
    assertEquals(
        "date", ReadSplitGenerator.getBucketForValue(new BsonDateTime(1700000000000L)).getName());
    assertEquals(
        "binData",
        ReadSplitGenerator.getBucketForValue(new BsonBinary(new byte[] {1, 2, 3})).getName());
    assertEquals(
        "object",
        ReadSplitGenerator.getBucketForValue(new BsonDocument("sub", new BsonInt32(1))).getName());
    assertEquals("null", ReadSplitGenerator.getBucketForValue(BsonNull.VALUE).getName());
    assertEquals(
        "timestamp", ReadSplitGenerator.getBucketForValue(new BsonTimestamp(100, 1)).getName());
  }

  @Test
  public void probeActiveTypeBounds_homogeneous_singleTypeImmediateReturn() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    FindIterable<BsonDocument> mockFind = mockChainedFind();
    when(mockCol.find()).thenReturn(mockFind);

    BsonObjectId minOid = new BsonObjectId(new ObjectId("600000000000000000000001"));
    BsonObjectId maxOid = new BsonObjectId(new ObjectId("600000000000000000000009"));

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
  public void probeActiveTypeBounds_mixedCollection_discoversAllActiveTypes() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    FindIterable<BsonDocument> mockGlobalFind = mockChainedFind();
    FindIterable<BsonDocument> mockBucketFind = mockChainedFind();
    when(mockCol.find()).thenReturn(mockGlobalFind);
    when(mockCol.find(any(BsonDocument.class))).thenReturn(mockBucketFind);

    BsonInt32 minNum = new BsonInt32(1);
    BsonInt32 maxNum = new BsonInt32(500);
    BsonString minStr = new BsonString("a");
    BsonString maxStr = new BsonString("z");
    BsonObjectId minOid = new BsonObjectId(new ObjectId("600000000000000000000001"));
    BsonObjectId maxOid = new BsonObjectId(new ObjectId("600000000000000000000009"));

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
  public void probeActiveTypeBounds_intermediateTimeout_abortsWithoutSilentDrop() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    FindIterable<BsonDocument> mockGlobalFind = mockChainedFind();
    FindIterable<BsonDocument> mockBucketFind = mockChainedFind();
    when(mockCol.find()).thenReturn(mockGlobalFind);
    when(mockCol.find(any(BsonDocument.class))).thenReturn(mockBucketFind);

    when(mockGlobalFind.first())
        .thenReturn(new BsonDocument("_id", new BsonInt32(1)))
        .thenReturn(
            new BsonDocument("_id", new BsonObjectId(new ObjectId("600000000000000000000009"))));

    // Simulate timeout exception on intermediate bucket probe
    when(mockBucketFind.first())
        .thenReturn(new BsonDocument("_id", new BsonInt32(100)))
        .thenThrow(new RuntimeException("MongoExecutionTimeoutException"));

    List<ReadSplitGenerator.ProbedTypeBounds> bounds =
        ReadSplitGenerator.probeActiveTypeBounds(mockCol);

    // Must abort and return empty list rather than silently dropping the timed-out type!
    assertTrue(bounds.isEmpty());
  }

  @Test
  public void generateProbedNumberSplits_handlesNegativeNumbersAndLarge64BitLongs() {
    List<BsonDocument> splits =
        ReadSplitGenerator.generateProbedNumberSplits(
            new BsonInt64(-1000L), new BsonInt64(1000L), 4);
    assertEquals(4, splits.size());
    assertTrue(splits.get(0).toJson().contains("\"$lt\""));
    assertFalse(splits.get(0).toJson().contains("\"$gte\""));
    assertTrue(splits.get(3).toJson().contains("\"$gte\""));
    assertFalse(splits.get(3).toJson().contains("\"$lt\""));
  }

  @Test
  public void probeActiveTypeBounds_emptyCollectionReturnsEmptyList() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    FindIterable<BsonDocument> mockFind = mockChainedFind();
    when(mockCol.find()).thenReturn(mockFind);
    when(mockFind.first()).thenReturn(null);

    List<ReadSplitGenerator.ProbedTypeBounds> bounds =
        ReadSplitGenerator.probeActiveTypeBounds(mockCol);

    assertTrue(bounds.isEmpty());
  }

  @Test
  public void generateProbedNumberSplits_preservesDoubleAndDecimal128Precision() {
    List<BsonDocument> doubleSplits =
        ReadSplitGenerator.generateProbedNumberSplits(new BsonDouble(0.0), new BsonDouble(1.0), 4);
    assertEquals(4, doubleSplits.size());
    assertTrue(doubleSplits.get(0).toJson().contains("0.25"));
    assertTrue(doubleSplits.get(1).toJson().contains("0.25"));
    assertTrue(doubleSplits.get(1).toJson().contains("0.5"));
    assertTrue(doubleSplits.get(3).toJson().contains("0.75"));

    List<BsonDocument> decSplits =
        ReadSplitGenerator.generateProbedNumberSplits(
            new BsonDecimal128(new Decimal128(new BigDecimal("0.0001"))),
            new BsonDecimal128(new Decimal128(new BigDecimal("0.0005"))),
            4);
    assertEquals(4, decSplits.size());
    assertTrue(decSplits.get(0).toJson().contains("0.0002"));
    assertTrue(decSplits.get(3).toJson().contains("0.0004"));

    // Degenerate range (min >= max or numSplits <= 1) returns single unsplit number filter
    List<BsonDocument> singleSplit =
        ReadSplitGenerator.generateProbedNumberSplits(new BsonDouble(5.0), new BsonDouble(5.0), 4);
    assertEquals(1, singleSplit.size());

    // Negative zero (-0) in Decimal128 maps to BigDecimal.ZERO and interpolates normally with a
    // finite positive max bound
    List<BsonDocument> negZeroToPosSplits =
        ReadSplitGenerator.generateProbedNumberSplits(
            new BsonDecimal128(Decimal128.NEGATIVE_ZERO),
            new BsonDecimal128(new Decimal128(new BigDecimal("100"))),
            4);
    assertEquals(4, negZeroToPosSplits.size());
    assertTrue(negZeroToPosSplits.get(0).toJson().contains("25"));

    // Non-finite bounds (NaN, +/-Infinity) cannot be linearly interpolated even when paired with a
    // finite bound (e.g. min=NaN, max=100 or min=-100, max=+Infinity) and must fall back to the
    // single unsplit $type filter so NaN/Infinity documents are never excluded by $lt/$gte bounds.
    List<BsonDocument> nanMinWithFiniteMaxSplits =
        ReadSplitGenerator.generateProbedNumberSplits(
            new BsonDecimal128(Decimal128.NaN),
            new BsonDecimal128(new Decimal128(new BigDecimal("100"))),
            4);
    assertEquals(1, nanMinWithFiniteMaxSplits.size());
    assertFalse(nanMinWithFiniteMaxSplits.get(0).toJson().contains("$lt"));

    List<BsonDocument> finiteMinWithPosInfMaxSplits =
        ReadSplitGenerator.generateProbedNumberSplits(
            new BsonDouble(-100.0), new BsonDouble(Double.POSITIVE_INFINITY), 4);
    assertEquals(1, finiteMinWithPosInfMaxSplits.size());
    assertFalse(finiteMinWithPosInfMaxSplits.get(0).toJson().contains("$lt"));

    List<BsonDocument> doubleNanMinWithFiniteMaxSplits =
        ReadSplitGenerator.generateProbedNumberSplits(
            new BsonDouble(Double.NaN), new BsonDouble(100.0), 4);
    assertEquals(1, doubleNanMinWithFiniteMaxSplits.size());

    // Integer range smaller than numSplits (step == 0) returns single unsplit number filter
    List<BsonDocument> smallIntRangeSplits =
        ReadSplitGenerator.generateProbedNumberSplits(new BsonInt32(1), new BsonInt32(2), 4);
    assertEquals(1, smallIntRangeSplits.size());

    // ObjectId range smaller than numSplits (step == 0) returns single unsplit objectId filter
    List<BsonDocument> smallOidRangeSplits =
        ReadSplitGenerator.generateProbedObjectIdSplits(
            "600000000000000000000001", "600000000000000000000002", 4);
    assertEquals(1, smallOidRangeSplits.size());
  }

  @Test
  public void generateFilterStrings_allCanonicalBsonTypesSupported() {
    List<BsonValue> sampleDocs =
        Arrays.asList(
            new BsonMinKey(),
            new BsonUndefined(),
            new BsonNull(),
            new BsonInt32(10),
            new BsonString("abc"),
            new BsonSymbol("sym"),
            new BsonDocument("k", new BsonInt32(1)),
            new BsonBinary(new byte[] {1, 2}),
            new BsonObjectId(new ObjectId("600000000000000000000001")),
            new BsonBoolean(false),
            new BsonDateTime(1000L),
            new BsonTimestamp(100, 1),
            new BsonRegularExpression("^a"),
            new BsonDbPointer("db.col", new ObjectId("600000000000000000000002")),
            new BsonJavaScript("function() { return 1; }"),
            new BsonJavaScriptWithScope(
                "function() { return x; }", new BsonDocument("x", new BsonInt32(1))),
            new BsonMaxKey());

    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    FindIterable<BsonDocument> mockGlobalFind = mockChainedFind();
    FindIterable<BsonDocument> mockBucketFind = mockChainedFind();
    when(mockCol.find()).thenReturn(mockGlobalFind);
    when(mockGlobalFind.first())
        .thenReturn(new BsonDocument("_id", new BsonMinKey()))
        .thenReturn(new BsonDocument("_id", new BsonMaxKey()));

    when(mockCol.find(any(Bson.class))).thenReturn(mockBucketFind);

    // minBucket (minKey) max is minKey; intermediate buckets each return their sample value for min
    // & max; maxBucket (maxKey) min is maxKey
    when(mockBucketFind.first())
        .thenReturn(new BsonDocument("_id", sampleDocs.get(0))) // minBucketMax
        .thenReturn(new BsonDocument("_id", sampleDocs.get(1))) // undefined min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(1))) // undefined max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(2))) // null min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(2))) // null max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(3))) // number min
        .thenReturn(new BsonDocument("_id", new BsonInt32(20))) // number max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(4))) // string min
        .thenReturn(new BsonDocument("_id", new BsonString("xyz"))) // string max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(5))) // symbol min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(5))) // symbol max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(6))) // object min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(6))) // object max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(7))) // binData min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(7))) // binData max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(8))) // objectId min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(8))) // objectId max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(9))) // bool min
        .thenReturn(new BsonDocument("_id", new BsonBoolean(true))) // bool max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(10))) // date min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(10))) // date max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(11))) // timestamp min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(11))) // timestamp max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(12))) // regex min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(12))) // regex max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(13))) // dbPointer min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(13))) // dbPointer max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(14))) // javascript min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(14))) // javascript max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(15))) // javascriptWithScope min
        .thenReturn(new BsonDocument("_id", sampleDocs.get(15))) // javascriptWithScope max
        .thenReturn(new BsonDocument("_id", sampleDocs.get(16))); // maxBucketMin

    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    when(mockClient.getDatabase("db")).thenReturn(mockDb);
    when(mockDb.getCollection("col", BsonDocument.class)).thenReturn(mockCol);
    when(mockDb.runCommand(any())).thenReturn(new Document("count", 10000));

    @SuppressWarnings("unchecked")
    AggregateIterable<BsonDocument> mockAgg = mock(AggregateIterable.class);
    @SuppressWarnings("unchecked")
    MongoCursor<BsonDocument> mockCursor = mock(MongoCursor.class);
    when(mockCol.aggregate(any())).thenReturn(mockAgg);
    when(mockAgg.allowDiskUse(any(Boolean.class))).thenReturn(mockAgg);
    when(mockAgg.maxTime(any(Long.class), any())).thenReturn(mockAgg);
    when(mockAgg.iterator()).thenReturn(mockCursor);

    // Feed all sampleDocs through $sample cursor to exercise matchesType for every BSON type
    List<BsonDocument> wrappedDocs = new ArrayList<>();
    for (BsonValue v : sampleDocs) {
      wrappedDocs.add(new BsonDocument("_id", v));
    }
    Iterator<BsonDocument> it = wrappedDocs.iterator();
    when(mockCursor.hasNext()).thenAnswer(inv -> it.hasNext());
    when(mockCursor.next()).thenAnswer(inv -> it.next());

    List<BsonDocument> splits =
        ReadSplitGenerator.generateIndexSliceFilters(mockClient, "db", "col", 26);
    assertFalse(splits.isEmpty());
  }

  @Test
  public void generateIndexSliceFilters_stringOnlyExceeding62Splits_omitsEmptyDocuments() {
    List<BsonDocument> filters =
        ReadSplitGenerator.generateIndexSliceFilters(
            128, EnumSet.of(ReadSplitGenerator.IdType.STRING));
    assertEquals(62, filters.size());
    for (BsonDocument filter : filters) {
      assertFalse("Must not emit empty full-collection-scan filter", filter.isEmpty());
      assertTrue(filter.toJson().contains("\"$type\": \"string\""));
    }
  }

  @Test
  public void probeActiveTypeBounds_symbolMinAndStringMax_discoversBothSharedRankTypes() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    FindIterable<BsonDocument> mockGlobalFind = mockChainedFind();
    FindIterable<BsonDocument> mockBucketFind = mockChainedFind();
    when(mockCol.find()).thenReturn(mockGlobalFind);
    when(mockCol.find(any(BsonDocument.class))).thenReturn(mockBucketFind);

    BsonSymbol minSym = new BsonSymbol("a_sym");
    BsonSymbol maxSym = new BsonSymbol("m_sym");
    BsonString minStr = new BsonString("b_str");
    BsonString maxStr = new BsonString("z_str");

    // Global min is Symbol("a_sym") (idx 4), global max is String("z_str") (idx 3)
    when(mockGlobalFind.first())
        .thenReturn(new BsonDocument("_id", minSym))
        .thenReturn(new BsonDocument("_id", maxStr));

    when(mockBucketFind.first())
        .thenReturn(new BsonDocument("_id", maxSym))
        .thenReturn(new BsonDocument("_id", minStr));

    List<ReadSplitGenerator.ProbedTypeBounds> bounds =
        ReadSplitGenerator.probeActiveTypeBounds(mockCol);

    assertEquals(2, bounds.size());
    assertEquals("symbol", bounds.get(0).getBucket().getName());
    assertEquals(minSym, bounds.get(0).getMinKey());
    assertEquals(maxSym, bounds.get(0).getMaxKey());
    assertEquals("string", bounds.get(1).getBucket().getName());
    assertEquals(minStr, bounds.get(1).getMinKey());
    assertEquals(maxStr, bounds.get(1).getMaxKey());
  }

  @Test
  public void generateTypeIsolatedSplits_nanBoundsWithSampledKeys_returnsSingleNumberFilter() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    FindIterable<BsonDocument> mockGlobalFind = mockChainedFind();
    when(mockCol.estimatedDocumentCount()).thenReturn(100_000L);
    when(mockCol.find()).thenReturn(mockGlobalFind);
    when(mockGlobalFind.first())
        .thenReturn(new BsonDocument("_id", new BsonDouble(Double.NaN)))
        .thenReturn(new BsonDocument("_id", new BsonDouble(100.0)));

    @SuppressWarnings("unchecked")
    AggregateIterable<BsonDocument> mockAgg = mock(AggregateIterable.class);
    @SuppressWarnings("unchecked")
    MongoCursor<BsonDocument> mockCursor = mock(MongoCursor.class);
    when(mockCol.aggregate(any())).thenReturn(mockAgg);
    when(mockAgg.maxTime(any(Long.class), any())).thenReturn(mockAgg);
    when(mockAgg.iterator()).thenReturn(mockCursor);

    List<BsonDocument> sampled =
        Arrays.asList(
            new BsonDocument("_id", new BsonDouble(1.0)),
            new BsonDocument("_id", new BsonDouble(50.0)),
            new BsonDocument("_id", new BsonDouble(100.0)));
    Iterator<BsonDocument> it = sampled.iterator();
    when(mockCursor.hasNext()).thenAnswer(inv -> it.hasNext());
    when(mockCursor.next()).thenAnswer(inv -> it.next());

    List<BsonDocument> splits = ReadSplitGenerator.generateTypeIsolatedSplits(mockCol, 4);
    assertEquals(1, splits.size());
    assertFalse(splits.get(0).toJson().contains("$lte"));
    assertFalse(splits.get(0).toJson().contains("$gt"));
  }

  @Test
  public void
      generateCounterWeightedObjectIdBoundaries_eliminatesSampleSkewAndPreservesProcessUnique() {
    String proc = "9e6b535a76";
    String minHex = "6ae24591" + proc + "000000";
    String maxHex = "6ae24591" + proc + "0003e8"; // counter = 1000

    // Simulate heavily skewed WiredTiger $sample where all samples cluster in counter [10..50]
    // and completely miss [51..1000].
    List<BsonValue> skewedSample =
        Arrays.asList(
            new BsonObjectId(new ObjectId("6ae24591" + proc + "00000a")),
            new BsonObjectId(new ObjectId("6ae24591" + proc + "000014")),
            new BsonObjectId(new ObjectId("6ae24591" + proc + "00001e")),
            new BsonObjectId(new ObjectId("6ae24591" + proc + "000032")));

    List<BsonValue> boundaries =
        ReadSplitGenerator.generateCounterWeightedObjectIdBoundaries(
            null, minHex, maxHex, 4, skewedSample);

    assertEquals(3, boundaries.size());
    // Ideal boundaries for counter [0..1000] split into 4 parts are at 250 (0xfa), 500 (0x1f4), 750
    // (0x2ee)
    assertEquals(
        "6ae24591" + proc + "0000fa", boundaries.get(0).asObjectId().getValue().toHexString());
    assertEquals(
        "6ae24591" + proc + "0001f4", boundaries.get(1).asObjectId().getValue().toHexString());
    assertEquals(
        "6ae24591" + proc + "0002ee", boundaries.get(2).asObjectId().getValue().toHexString());
  }

  @Test
  public void probeActiveTypeBounds_sharedRankStringAndSymbol_discoversSiblingAcrossAllLayouts() {
    // 1. Homogeneous String global bounds [String('a'), Symbol('m'), String('z')]
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> strWrappedCol = mock(MongoCollection.class);
    FindIterable<BsonDocument> strGlobalFind = mockChainedFind();
    FindIterable<BsonDocument> strBucketFind = mockChainedFind();
    when(strWrappedCol.find()).thenReturn(strGlobalFind);
    when(strWrappedCol.find(any(BsonDocument.class))).thenReturn(strBucketFind);
    when(strGlobalFind.first())
        .thenReturn(new BsonDocument("_id", new BsonString("a")))
        .thenReturn(new BsonDocument("_id", new BsonString("z")));
    when(strBucketFind.first())
        .thenReturn(new BsonDocument("_id", new BsonSymbol("m")))
        .thenReturn(new BsonDocument("_id", new BsonSymbol("m")));

    List<ReadSplitGenerator.ProbedTypeBounds> strWrappedBounds =
        ReadSplitGenerator.probeActiveTypeBounds(strWrappedCol);
    assertEquals(2, strWrappedBounds.size());
    assertEquals("string", strWrappedBounds.get(0).getBucket().getName());
    assertEquals("symbol", strWrappedBounds.get(1).getBucket().getName());

    // 2. Homogeneous Symbol global bounds [Symbol('a'), String('m'), Symbol('z')]
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> symWrappedCol = mock(MongoCollection.class);
    FindIterable<BsonDocument> symGlobalFind = mockChainedFind();
    FindIterable<BsonDocument> symBucketFind = mockChainedFind();
    when(symWrappedCol.find()).thenReturn(symGlobalFind);
    when(symWrappedCol.find(any(BsonDocument.class))).thenReturn(symBucketFind);
    when(symGlobalFind.first())
        .thenReturn(new BsonDocument("_id", new BsonSymbol("a")))
        .thenReturn(new BsonDocument("_id", new BsonSymbol("z")));
    when(symBucketFind.first())
        .thenReturn(new BsonDocument("_id", new BsonString("m")))
        .thenReturn(new BsonDocument("_id", new BsonString("m")));

    List<ReadSplitGenerator.ProbedTypeBounds> symWrappedBounds =
        ReadSplitGenerator.probeActiveTypeBounds(symWrappedCol);
    assertEquals(2, symWrappedBounds.size());
    assertEquals("symbol", symWrappedBounds.get(0).getBucket().getName());
    assertEquals("string", symWrappedBounds.get(1).getBucket().getName());

    // 3. Mixed Number..String global bounds [Int32(1), Symbol('a'), String('z')]
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> numToStrCol = mock(MongoCollection.class);
    FindIterable<BsonDocument> numStrGlobal = mockChainedFind();
    FindIterable<BsonDocument> numStrBucket = mockChainedFind();
    when(numToStrCol.find()).thenReturn(numStrGlobal);
    when(numToStrCol.find(any(BsonDocument.class))).thenReturn(numStrBucket);
    when(numStrGlobal.first())
        .thenReturn(new BsonDocument("_id", new BsonInt32(1)))
        .thenReturn(new BsonDocument("_id", new BsonString("z")));
    when(numStrBucket.first())
        .thenReturn(new BsonDocument("_id", new BsonInt32(1))) // number max
        .thenReturn(new BsonDocument("_id", new BsonString("z"))) // string min
        .thenReturn(new BsonDocument("_id", new BsonSymbol("a"))) // symbol min
        .thenReturn(new BsonDocument("_id", new BsonSymbol("a"))); // symbol max

    List<ReadSplitGenerator.ProbedTypeBounds> numToStrBounds =
        ReadSplitGenerator.probeActiveTypeBounds(numToStrCol);
    assertEquals(3, numToStrBounds.size());
    assertEquals("number", numToStrBounds.get(0).getBucket().getName());
    assertEquals("string", numToStrBounds.get(1).getBucket().getName());
    assertEquals("symbol", numToStrBounds.get(2).getBucket().getName());

    // 4. Mixed Symbol..ObjectId global bounds [Symbol('a'), String('z'), ObjectId]
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> symToOidCol = mock(MongoCollection.class);
    FindIterable<BsonDocument> symOidGlobal = mockChainedFind();
    FindIterable<BsonDocument> symOidBucket = mockChainedFind();
    BsonObjectId oid = new BsonObjectId(new ObjectId("600000000000000000000001"));
    when(symToOidCol.find()).thenReturn(symOidGlobal);
    when(symToOidCol.find(any(BsonDocument.class))).thenReturn(symOidBucket);
    when(symOidGlobal.first())
        .thenReturn(new BsonDocument("_id", new BsonSymbol("a")))
        .thenReturn(new BsonDocument("_id", oid));
    when(symOidBucket.first())
        .thenReturn(new BsonDocument("_id", new BsonSymbol("a"))) // symbol max
        .thenReturn(new BsonDocument("_id", new BsonString("z"))) // string sibling min
        .thenReturn(new BsonDocument("_id", new BsonString("z"))) // string sibling max
        .thenReturn(null) // object min
        .thenReturn(null) // binData min
        .thenReturn(new BsonDocument("_id", oid)); // objectId min

    List<ReadSplitGenerator.ProbedTypeBounds> symToOidBounds =
        ReadSplitGenerator.probeActiveTypeBounds(symToOidCol);
    assertEquals(3, symToOidBounds.size());
    assertEquals("symbol", symToOidBounds.get(0).getBucket().getName());
    assertEquals("string", symToOidBounds.get(1).getBucket().getName());
    assertEquals("objectId", symToOidBounds.get(2).getBucket().getName());
  }

  @Test
  public void generateCounterWeightedObjectIdBoundaries_sameSecondWraparound_avoidsEmptyGap() {
    String proc = "9e6b535a76";
    // Counter initialized at 0xfffff0 wraps past 0xffffff to 0x000100 within the same second.
    String minHex = "6ae24591" + proc + "000000";
    String maxHex = "6ae24591" + proc + "ffffff";
    List<BsonValue> sample =
        Arrays.asList(
            new BsonObjectId(new ObjectId("6ae24591" + proc + "000080")),
            new BsonObjectId(new ObjectId("6ae24591" + proc + "000100")),
            new BsonObjectId(new ObjectId("6ae24591" + proc + "fffff0")),
            new BsonObjectId(new ObjectId("6ae24591" + proc + "fffff8")));

    List<BsonValue> boundaries =
        ReadSplitGenerator.generateCounterWeightedObjectIdBoundaries(
            null, minHex, maxHex, 4, sample);

    assertEquals(3, boundaries.size());
    for (BsonValue b : boundaries) {
      String hex = b.asObjectId().getValue().toHexString();
      long ctr = Long.parseLong(hex.substring(18, 24), 16);
      assertTrue(
          "Boundary must not fall inside unoccupied wraparound gap (0x000100, 0xfffff0): " + hex,
          ctr <= 0x000100L || ctr >= 0xfffff0L);
    }
  }

  @Test
  public void generateTypeIsolatedSplits_singleChunkMixedTypes_isolatesEachActiveType() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    when(mockCol.estimatedDocumentCount()).thenReturn(100_000L);
    FindIterable<BsonDocument> globalFind = mockChainedFind();
    FindIterable<BsonDocument> bucketFind = mockChainedFind();
    when(mockCol.find()).thenReturn(globalFind);
    when(mockCol.find(any(BsonDocument.class))).thenReturn(bucketFind);

    BsonInt32 numId = new BsonInt32(1);
    BsonObjectId oid = new BsonObjectId(new ObjectId("600000000000000000000001"));
    when(globalFind.first())
        .thenReturn(new BsonDocument("_id", numId))
        .thenReturn(new BsonDocument("_id", oid));
    when(bucketFind.first())
        .thenReturn(new BsonDocument("_id", new BsonInt32(500))) // number max
        .thenReturn(null) // string
        .thenReturn(null) // symbol
        .thenReturn(null) // object
        .thenReturn(null) // binData
        .thenReturn(new BsonDocument("_id", oid)); // objectId min

    // Even when estimatedDocs (100k) <= targetChunkSize (200k), mixed-type collections must emit
    // per-type filters so keyset ($gt) resumption never gets type-bracketed to a single BSON type.
    List<BsonDocument> splits =
        ReadSplitGenerator.generateTypeIsolatedSplits(mockCol, 1, 200_000, 256);
    assertEquals(2, splits.size());
    assertTrue(splits.get(0).toJson().contains("\"int\""));
    assertTrue(splits.get(1).toJson().contains("\"objectId\""));
  }

  @Test
  public void generateProbedNumberSplits_subnormalAndExtremeDecimal128_handlesWithoutCrash() {
    List<BsonDocument> subnormalSplits =
        ReadSplitGenerator.generateProbedNumberSplits(
            new BsonDecimal128(Decimal128.parse("0")),
            new BsonDecimal128(Decimal128.parse("1E-6176")),
            4);
    assertEquals(1, subnormalSplits.size());
    assertFalse(subnormalSplits.get(0).toJson().contains("$lt"));

    // Full-range Decimal128 span (-9E6144 to +9E6144, range=1.8E6145 > MAX_DECIMAL128) still
    // computes valid in-range boundaries (-4.5E6144, 0, +4.5E6144) via BigDecimal without overflow.
    List<BsonDocument> extremeRangeSplits =
        ReadSplitGenerator.generateProbedNumberSplits(
            new BsonDecimal128(Decimal128.parse("-9E6144")),
            new BsonDecimal128(Decimal128.parse("9E6144")),
            4);
    assertEquals(4, extremeRangeSplits.size());
  }

  @Test
  public void getBucketForValue_identifiesUndefinedDbPointerAndJavaScriptTypes() {
    assertEquals("undefined", ReadSplitGenerator.getBucketForValue(new BsonUndefined()).getName());
    assertEquals(
        "dbPointer",
        ReadSplitGenerator.getBucketForValue(
                new BsonDbPointer("db.col", new ObjectId("600000000000000000000001")))
            .getName());
    assertEquals(
        "javascript", ReadSplitGenerator.getBucketForValue(new BsonJavaScript("f()")).getName());
    assertEquals(
        "javascriptWithScope",
        ReadSplitGenerator.getBucketForValue(new BsonJavaScriptWithScope("f()", new BsonDocument()))
            .getName());
  }

  @SuppressWarnings("unchecked")
  private static FindIterable<BsonDocument> mockChainedFind() {
    FindIterable<BsonDocument> mockFind = mock(FindIterable.class);
    when(mockFind.projection(any())).thenReturn(mockFind);
    when(mockFind.sort(any())).thenReturn(mockFind);
    when(mockFind.limit(any(Integer.class))).thenReturn(mockFind);
    when(mockFind.maxTime(any(Long.class), any())).thenReturn(mockFind);
    return mockFind;
  }
}
