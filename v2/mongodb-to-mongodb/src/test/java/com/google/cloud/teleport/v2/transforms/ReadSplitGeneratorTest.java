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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

    when(mockClient.getDatabase(anyString())).thenReturn(mockDb);
    when(mockDb.getCollection(anyString(), eq(BsonDocument.class))).thenReturn(mockCol);
    when(mockCol.estimatedDocumentCount()).thenReturn(50_000L);

    FindIterable<BsonDocument> mockGlobalFind = mockChainedFind();
    FindIterable<BsonDocument> mockBucketFind = mockChainedFind();
    when(mockCol.find()).thenReturn(mockGlobalFind);
    when(mockCol.find(any(BsonDocument.class))).thenReturn(mockBucketFind);

    // Global min = Int32(1), global max = String("z")
    when(mockGlobalFind.first())
        .thenReturn(new BsonDocument("_id", new BsonInt32(1)))
        .thenReturn(new BsonDocument("_id", new BsonString("z")));
    // number max = Int32(100), string min = String("a"), symbol sibling min = null
    when(mockBucketFind.first())
        .thenReturn(new BsonDocument("_id", new BsonInt32(100)))
        .thenReturn(new BsonDocument("_id", new BsonString("a")))
        .thenReturn(null);

    mockSampledDocuments(
        mockCol,
        Arrays.asList(
            new BsonDocument("_id", new BsonInt32(1)),
            new BsonDocument("_id", new BsonInt32(25)),
            new BsonDocument("_id", new BsonInt32(50)),
            new BsonDocument("_id", new BsonInt32(100)),
            new BsonDocument("_id", new BsonString("a")),
            new BsonDocument("_id", new BsonString("f")),
            new BsonDocument("_id", new BsonString("m")),
            new BsonDocument("_id", new BsonString("z"))));

    List<BsonDocument> filters =
        ReadSplitGenerator.generateIndexSliceFilters(mockClient, "db", "col", 4);

    assertEquals(4, filters.size());
    for (BsonDocument filter : filters) {
      assertFalse(
          "Type-isolated splits must never combine types with $or",
          filter.toJson().contains("\"$or\""));
    }
    assertEquals(new BsonInt32(50), filters.get(0).getDocument("_id").get("$lte"));
    assertEquals(new BsonInt32(50), filters.get(1).getDocument("_id").get("$gt"));
    assertEquals(new BsonString("m"), filters.get(2).getDocument("_id").get("$lte"));
    assertEquals(new BsonString("m"), filters.get(3).getDocument("_id").get("$gt"));
  }

  @Test
  public void testGenerateTypeIsolatedSplits_smallCollectionReturnsSingleSplit() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    FindIterable<BsonDocument> mockFind = mockChainedFind();
    when(mockCol.estimatedDocumentCount()).thenReturn(2000L);
    when(mockCol.find()).thenReturn(mockFind);
    when(mockFind.first())
        .thenReturn(new BsonDocument("_id", new BsonString("a")))
        .thenReturn(new BsonDocument("_id", new BsonString("z")));
    FindIterable<BsonDocument> mockBucketFind = mockChainedFind();
    when(mockCol.find(any(BsonDocument.class))).thenReturn(mockBucketFind);
    when(mockBucketFind.first()).thenReturn(null);

    List<BsonDocument> splits = ReadSplitGenerator.generateTypeIsolatedSplits(mockCol, 16);
    assertEquals(1, splits.size());
    assertTrue(splits.get(0).isEmpty());
  }

  @Test
  public void testGenerateTypeIsolatedSplits_adaptiveVolumeSizing_calculatesEffectiveSplits() {
    MongoCollection<BsonDocument> mockCol = mockHomogeneousObjectIdCollectionWithCount(1_000_000L);

    List<BsonDocument> splits =
        ReadSplitGenerator.generateTypeIsolatedSplits(mockCol, 1, 200_000, 256);
    assertEquals(5, splits.size());
    for (BsonDocument split : splits) {
      assertFalse(split.toJson().contains("\"$or\""));
      assertTrue(split.toJson().contains("\"$type\": \"objectId\""));
    }
  }

  @Test
  public void testGenerateTypeIsolatedSplits_adaptiveVolumeSizing_clampsToMaxSplits() {
    MongoCollection<BsonDocument> mockCol =
        mockHomogeneousObjectIdCollectionWithCount(100_000_000L);

    List<BsonDocument> splits =
        ReadSplitGenerator.generateTypeIsolatedSplits(mockCol, 1, 200_000, 256);
    assertEquals(256, splits.size());
    assertFalse(splits.get(0).toJson().contains("\"$or\""));
  }

  @Test
  public void testGenerateTypeIsolatedSplits_zeroTargetChunkSize_usesNumSplits() {
    MongoCollection<BsonDocument> mockCol = mockHomogeneousObjectIdCollectionWithCount(10_000_000L);

    List<BsonDocument> splits = ReadSplitGenerator.generateTypeIsolatedSplits(mockCol, 16, 0, 256);
    assertEquals(16, splits.size());
    assertFalse(splits.get(0).toJson().contains("\"$or\""));
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

    // Simulate MongoExecutionTimeoutException on intermediate bucket probe
    when(mockBucketFind.first())
        .thenReturn(new BsonDocument("_id", new BsonInt32(100)))
        .thenThrow(new MongoExecutionTimeoutException(50, "Execution timed out"));

    List<ReadSplitGenerator.ProbedTypeBounds> bounds =
        ReadSplitGenerator.probeActiveTypeBounds(mockCol);

    // Must abort and return empty list rather than silently dropping the timed-out type!
    assertTrue(bounds.isEmpty());

    // Non-MongoException programming bugs must propagate rather than being silently masked
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> buggyCol = mock(MongoCollection.class);
    when(buggyCol.find()).thenThrow(new IllegalStateException("unexpected bug"));
    assertThrows(
        IllegalStateException.class, () -> ReadSplitGenerator.probeActiveTypeBounds(buggyCol));
  }

  @Test
  public void generateProbedNumberSplits_handlesNegativeNumbersAndLarge64BitLongs() {
    List<BsonDocument> splits =
        ReadSplitGenerator.generateProbedNumberSplits(
            new BsonInt64(-1000L), new BsonInt64(1000L), 4);
    assertEquals(4, splits.size());
    assertEquals(new BsonInt64(-500L), splits.get(0).getDocument("_id").get("$lt"));
    assertFalse(splits.get(0).getDocument("_id").containsKey("$gte"));
    assertEquals(new BsonInt64(-500L), splits.get(1).getDocument("_id").get("$gte"));
    assertEquals(new BsonInt64(0L), splits.get(1).getDocument("_id").get("$lt"));
    assertEquals(new BsonInt64(0L), splits.get(2).getDocument("_id").get("$gte"));
    assertEquals(new BsonInt64(500L), splits.get(2).getDocument("_id").get("$lt"));
    assertEquals(new BsonInt64(500L), splits.get(3).getDocument("_id").get("$gte"));
    assertFalse(splits.get(3).getDocument("_id").containsKey("$lt"));

    // Values beyond 2^53 (9_007_199_254_740_992L) must preserve exact 64-bit integer precision
    long above53Min = (1L << 53) + 100L;
    long above53Max = (1L << 53) + 500L;
    List<BsonDocument> largeLongSplits =
        ReadSplitGenerator.generateProbedNumberSplits(
            new BsonInt64(above53Min), new BsonInt64(above53Max), 4);
    assertEquals(4, largeLongSplits.size());
    assertEquals(
        new BsonInt64((1L << 53) + 200L), largeLongSplits.get(0).getDocument("_id").get("$lt"));
    assertEquals(
        new BsonInt64((1L << 53) + 200L), largeLongSplits.get(1).getDocument("_id").get("$gte"));
    assertEquals(
        new BsonInt64((1L << 53) + 300L), largeLongSplits.get(1).getDocument("_id").get("$lt"));
    assertEquals(
        new BsonInt64((1L << 53) + 300L), largeLongSplits.get(2).getDocument("_id").get("$gte"));
    assertEquals(
        new BsonInt64((1L << 53) + 400L), largeLongSplits.get(2).getDocument("_id").get("$lt"));
    assertEquals(
        new BsonInt64((1L << 53) + 400L), largeLongSplits.get(3).getDocument("_id").get("$gte"));
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
    when(mockCol.estimatedDocumentCount()).thenReturn(100_000L);

    // Feed all sampleDocs through $sample cursor to exercise matchesType for every BSON type
    List<BsonDocument> wrappedDocs = new ArrayList<>();
    for (BsonValue v : sampleDocs) {
      wrappedDocs.add(new BsonDocument("_id", v));
    }
    mockSampledDocuments(mockCol, wrappedDocs);

    // 17 active types: 15 single-split buckets (14 minKey==maxKey + bool) + 2 multi-doc buckets
    // (number [10..20], string ["abc".."xyz"]). remainingSplits = 21 - 15 = 6 -> 3 splits each,
    // yielding 15 + 3 + 3 = 21 total splits in strict canonical BSON order.
    List<BsonDocument> splits =
        ReadSplitGenerator.generateIndexSliceFilters(mockClient, "db", "col", 21);
    assertEquals(21, splits.size());
    assertTrue(splits.get(0).toJson().contains("\"minKey\""));
    assertTrue(splits.get(1).toJson().contains("\"undefined\""));
    assertTrue(splits.get(2).toJson().contains("\"null\""));
    // Indices 3..5: 3 interpolated number splits
    assertTrue(splits.get(3).toJson().contains("\"int\""));
    assertTrue(splits.get(5).toJson().contains("\"int\""));
    // Indices 6..8: 3 string splits
    assertTrue(splits.get(6).toJson().contains("\"string\""));
    assertTrue(splits.get(8).toJson().contains("\"string\""));
    // Indices 9..20: remaining 12 single-split buckets in exact canonical order
    assertTrue(splits.get(9).toJson().contains("\"symbol\""));
    assertTrue(splits.get(10).toJson().contains("\"object\""));
    assertTrue(splits.get(11).toJson().contains("\"binData\""));
    assertTrue(splits.get(12).toJson().contains("\"objectId\""));
    assertTrue(splits.get(13).toJson().contains("\"bool\""));
    assertFalse(splits.get(13).toJson().contains("$lte"));
    assertTrue(splits.get(14).toJson().contains("\"date\""));
    assertTrue(splits.get(15).toJson().contains("\"timestamp\""));
    assertTrue(splits.get(16).toJson().contains("\"regex\""));
    assertTrue(splits.get(17).toJson().contains("\"dbPointer\""));
    assertTrue(splits.get(18).toJson().contains("\"javascript\""));
    assertTrue(splits.get(19).toJson().contains("\"javascriptWithScope\""));
    assertTrue(splits.get(20).toJson().contains("\"maxKey\""));
  }

  @Test
  public void generateIndexSliceFilters_stringOnlyExceeding62Splits_omitsEmptyDocuments() {
    List<BsonDocument> filters =
        ReadSplitGenerator.generateIndexSliceFilters(
            128, EnumSet.of(ReadSplitGenerator.IdType.STRING));
    assertEquals(62, filters.size());
    String prevGte = null;
    for (int i = 0; i < filters.size(); i++) {
      BsonDocument filter = filters.get(i);
      assertFalse("Must not emit empty full-collection-scan filter", filter.isEmpty());
      BsonDocument idDoc = filter.getDocument("_id");
      assertEquals(new BsonString("string"), idDoc.get("$type"));
      if (i > 0) {
        String gte = idDoc.getString("$gte").getValue();
        if (prevGte != null) {
          assertTrue("Bounds must be strictly increasing", gte.compareTo(prevGte) > 0);
        }
        prevGte = gte;
      }
      if (i < filters.size() - 1) {
        String lt = idDoc.getString("$lt").getValue();
        if (i > 0) {
          assertFalse(lt.equals(idDoc.getString("$gte").getValue()));
        }
      } else {
        assertFalse("Tail slice must be open-ended without $lt", idDoc.containsKey("$lt"));
        assertFalse("Tail slice must be open-ended without $lte", idDoc.containsKey("$lte"));
      }
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

  @Test
  public void generateTypeIsolatedSplits_quantileBoundariesAndDuplicateDedup_emitsOpenEndedTail() {
    // 1. typeKeys.size() >= splits (8 sampled string keys, splits = 4)
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> fullSampleCol = mock(MongoCollection.class);
    when(fullSampleCol.estimatedDocumentCount()).thenReturn(100_000L);
    FindIterable<BsonDocument> fullGlobal = mockChainedFind();
    FindIterable<BsonDocument> fullBucket = mockChainedFind();
    when(fullSampleCol.find()).thenReturn(fullGlobal);
    when(fullSampleCol.find(any(BsonDocument.class))).thenReturn(fullBucket);
    when(fullGlobal.first())
        .thenReturn(new BsonDocument("_id", new BsonString("k1")))
        .thenReturn(new BsonDocument("_id", new BsonString("k8")));
    when(fullBucket.first()).thenReturn(null);
    mockSampledDocuments(
        fullSampleCol,
        Arrays.asList(
            new BsonDocument("_id", new BsonString("k1")),
            new BsonDocument("_id", new BsonString("k2")),
            new BsonDocument("_id", new BsonString("k3")),
            new BsonDocument("_id", new BsonString("k4")),
            new BsonDocument("_id", new BsonString("k5")),
            new BsonDocument("_id", new BsonString("k6")),
            new BsonDocument("_id", new BsonString("k7")),
            new BsonDocument("_id", new BsonString("k8"))));

    List<BsonDocument> fullSplits = ReadSplitGenerator.generateTypeIsolatedSplits(fullSampleCol, 4);
    assertEquals(4, fullSplits.size());
    assertEquals(new BsonString("k3"), fullSplits.get(0).getDocument("_id").get("$lte"));
    assertFalse(fullSplits.get(0).getDocument("_id").containsKey("$gt"));
    assertEquals(new BsonString("k3"), fullSplits.get(1).getDocument("_id").get("$gt"));
    assertEquals(new BsonString("k5"), fullSplits.get(1).getDocument("_id").get("$lte"));
    assertEquals(new BsonString("k7"), fullSplits.get(3).getDocument("_id").get("$gt"));
    assertFalse(fullSplits.get(3).getDocument("_id").containsKey("$lte"));

    // 2. 2 <= typeKeys.size() < splits with duplicate boundaries ("a", "a", "a", "z", splits = 8)
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> dupSampleCol = mock(MongoCollection.class);
    when(dupSampleCol.estimatedDocumentCount()).thenReturn(100_000L);
    FindIterable<BsonDocument> dupGlobal = mockChainedFind();
    FindIterable<BsonDocument> dupBucket = mockChainedFind();
    when(dupSampleCol.find()).thenReturn(dupGlobal);
    when(dupSampleCol.find(any(BsonDocument.class))).thenReturn(dupBucket);
    when(dupGlobal.first())
        .thenReturn(new BsonDocument("_id", new BsonString("a")))
        .thenReturn(new BsonDocument("_id", new BsonString("z")));
    when(dupBucket.first()).thenReturn(null);
    mockSampledDocuments(
        dupSampleCol,
        Arrays.asList(
            new BsonDocument("_id", new BsonString("a")),
            new BsonDocument("_id", new BsonString("a")),
            new BsonDocument("_id", new BsonString("a")),
            new BsonDocument("_id", new BsonString("z"))));

    List<BsonDocument> dupSplits = ReadSplitGenerator.generateTypeIsolatedSplits(dupSampleCol, 8);
    // Duplicate "a" boundaries collapse so no empty {$gt: "a", $lte: "a"} partition is produced
    assertEquals(3, dupSplits.size());
    assertEquals(new BsonString("a"), dupSplits.get(0).getDocument("_id").get("$lte"));
    assertEquals(new BsonString("a"), dupSplits.get(1).getDocument("_id").get("$gt"));
    assertEquals(new BsonString("z"), dupSplits.get(1).getDocument("_id").get("$lte"));
    assertEquals(new BsonString("z"), dupSplits.get(2).getDocument("_id").get("$gt"));
  }

  @Test
  public void
      generateTypeIsolatedSplits_mixedCollectionProportionalWeightingAndSampleFailureFallback() {
    // 1. Skewed multi-type collection: 9 string samples + 1 number sample + bool (1-split budget)
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> skewedCol = mock(MongoCollection.class);
    when(skewedCol.estimatedDocumentCount()).thenReturn(100_000L);
    FindIterable<BsonDocument> skewedGlobal = mockChainedFind();
    FindIterable<BsonDocument> skewedBucket = mockChainedFind();
    when(skewedCol.find()).thenReturn(skewedGlobal);
    when(skewedCol.find(any(BsonDocument.class))).thenReturn(skewedBucket);
    when(skewedGlobal.first())
        .thenReturn(new BsonDocument("_id", new BsonInt32(1)))
        .thenReturn(new BsonDocument("_id", new BsonBoolean(true)));
    // Between number (idx 3) and bool (idx 9): string active, symbol..objectId empty
    when(skewedBucket.first())
        .thenReturn(new BsonDocument("_id", new BsonInt32(1000))) // number max
        .thenReturn(new BsonDocument("_id", new BsonString("s0"))) // string min
        .thenReturn(new BsonDocument("_id", new BsonString("s9"))) // string max
        .thenReturn(null) // symbol
        .thenReturn(null) // object
        .thenReturn(null) // binData
        .thenReturn(null) // objectId
        .thenReturn(new BsonDocument("_id", new BsonBoolean(false))); // bool min

    List<BsonDocument> skewedSamples = new ArrayList<>();
    skewedSamples.add(new BsonDocument("_id", new BsonInt32(50)));
    for (int i = 0; i < 9; i++) {
      skewedSamples.add(new BsonDocument("_id", new BsonString("s" + i)));
    }
    mockSampledDocuments(skewedCol, skewedSamples);

    // effectiveSplits = 11 -> bool gets 1, remainingSplits = 10 -> number gets 1 (1/10 * 10),
    // string gets 9 (9/10 * 10)
    List<BsonDocument> weightedSplits =
        ReadSplitGenerator.generateTypeIsolatedSplits(skewedCol, 11);
    int numFilterCount = 0;
    int strFilterCount = 0;
    int boolFilterCount = 0;
    for (BsonDocument f : weightedSplits) {
      String json = f.toJson();
      if (json.contains("\"int\"")) {
        numFilterCount++;
      } else if (json.contains("\"string\"")) {
        strFilterCount++;
      } else if (json.contains("\"bool\"")) {
        boolFilterCount++;
      }
    }
    assertEquals(1, numFilterCount);
    assertEquals(9, strFilterCount);
    assertEquals(1, boolFilterCount);

    // 2. When $sample fails with MongoException (totalMultiDocSamples == 0), falls back to equal
    // split allocation across multi-doc buckets using probed bounds
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> sampleFailCol = mock(MongoCollection.class);
    when(sampleFailCol.estimatedDocumentCount()).thenReturn(100_000L);
    FindIterable<BsonDocument> failGlobal = mockChainedFind();
    FindIterable<BsonDocument> failBucket = mockChainedFind();
    when(sampleFailCol.find()).thenReturn(failGlobal);
    when(sampleFailCol.find(any(BsonDocument.class))).thenReturn(failBucket);
    when(failGlobal.first())
        .thenReturn(new BsonDocument("_id", new BsonInt32(0)))
        .thenReturn(new BsonDocument("_id", new BsonString("z")));
    when(failBucket.first())
        .thenReturn(new BsonDocument("_id", new BsonInt32(400))) // number max
        .thenReturn(new BsonDocument("_id", new BsonString("a"))) // string min
        .thenReturn(null); // symbol sibling
    when(sampleFailCol.aggregate(any())).thenThrow(new MongoException("sample timed out"));

    List<BsonDocument> fallbackSplits =
        ReadSplitGenerator.generateTypeIsolatedSplits(sampleFailCol, 6);
    assertEquals(6, fallbackSplits.size());
  }

  @Test
  public void probeActiveTypeBounds_sharedRankNullAndUndefined_discoversBothBuckets() {
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    FindIterable<BsonDocument> globalFind = mockChainedFind();
    FindIterable<BsonDocument> bucketFind = mockChainedFind();
    when(mockCol.find()).thenReturn(globalFind);
    when(mockCol.find(any(BsonDocument.class))).thenReturn(bucketFind);

    // Global min = null (idx 2), global max = int32 (idx 3), while legacy undefined (idx 1) exists
    when(globalFind.first())
        .thenReturn(new BsonDocument("_id", BsonNull.VALUE))
        .thenReturn(new BsonDocument("_id", new BsonInt32(10)));
    when(bucketFind.first())
        .thenReturn(new BsonDocument("_id", BsonNull.VALUE)) // null max
        .thenReturn(new BsonDocument("_id", new BsonUndefined())) // undefined sibling min
        .thenReturn(new BsonDocument("_id", new BsonUndefined())) // undefined sibling max
        .thenReturn(new BsonDocument("_id", new BsonInt32(1))); // number min

    List<ReadSplitGenerator.ProbedTypeBounds> bounds =
        ReadSplitGenerator.probeActiveTypeBounds(mockCol);
    assertEquals(3, bounds.size());
    assertEquals("null", bounds.get(0).getBucket().getName());
    assertEquals("undefined", bounds.get(1).getBucket().getName());
    assertEquals("number", bounds.get(2).getBucket().getName());
  }

  @SuppressWarnings("unchecked")
  private static MongoCollection<BsonDocument> mockHomogeneousObjectIdCollectionWithCount(
      long estimatedCount) {
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);
    FindIterable<BsonDocument> mockFind = mockChainedFind();
    when(mockCol.estimatedDocumentCount()).thenReturn(estimatedCount);
    when(mockCol.find()).thenReturn(mockFind);
    when(mockFind.first())
        .thenReturn(
            new BsonDocument("_id", new BsonObjectId(new ObjectId("000000000000000000000001"))))
        .thenReturn(
            new BsonDocument("_id", new BsonObjectId(new ObjectId("ffffffffffffffffffffffff"))));
    mockSampledDocuments(mockCol, Collections.emptyList());
    return mockCol;
  }

  @SuppressWarnings("unchecked")
  private static void mockSampledDocuments(
      MongoCollection<BsonDocument> mockCol, List<BsonDocument> docs) {
    AggregateIterable<BsonDocument> mockAgg = mock(AggregateIterable.class);
    MongoCursor<BsonDocument> mockCursor = mock(MongoCursor.class);
    when(mockCol.aggregate(any())).thenReturn(mockAgg);
    when(mockAgg.maxTime(any(Long.class), any())).thenReturn(mockAgg);
    when(mockAgg.iterator()).thenReturn(mockCursor);
    Iterator<BsonDocument> it = docs.iterator();
    when(mockCursor.hasNext()).thenAnswer(inv -> it.hasNext());
    when(mockCursor.next()).thenAnswer(inv -> it.next());
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
