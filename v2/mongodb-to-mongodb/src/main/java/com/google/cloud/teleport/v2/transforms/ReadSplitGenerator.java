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

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.bson.BsonArray;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to generate orthogonal BSON filter queries for parallel index-slice reading without
 * requiring MongoDB splitVector or bucketAuto commands.
 */
public final class ReadSplitGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(ReadSplitGenerator.class);

  private ReadSplitGenerator() {}

  public enum IdType {
    STRING,
    OBJECT_ID,
    NUMBER,
    OTHER
  }

  /**
   * Generates a list of BsonDocument filter queries that partition a MongoDB collection across
   * default BSON data types (Numbers, Strings, ObjectIds, and remaining types).
   *
   * @param numSplits Total target number of parallel read splits.
   * @return List of BsonDocument filters.
   */
  public static List<BsonDocument> generateIndexSliceFilters(int numSplits) {
    return generateIndexSliceFilters(numSplits, EnumSet.allOf(IdType.class));
  }

  /**
   * Generates a list of BsonDocument filter queries using 2-phase covered index type discovery and
   * type-isolated quantile sampling.
   *
   * @param client MongoDB client connection.
   * @param databaseName Database name.
   * @param collectionName Collection name.
   * @param numSplits Number of target parallel read splits.
   * @return List of BsonDocument filters.
   */
  public static List<BsonDocument> generateIndexSliceFilters(
      MongoClient client, String databaseName, String collectionName, int numSplits) {
    return generateIndexSliceFilters(
        client, databaseName, collectionName, numSplits, 0, Math.max(numSplits, 256));
  }

  /**
   * Generates a list of BsonDocument filter queries with adaptive volume-based split sizing and
   * bounded upper bounds.
   *
   * @param client MongoDB client connection.
   * @param databaseName Database name.
   * @param collectionName Collection name.
   * @param numSplits Number of target parallel read splits (serves as floor/fallback).
   * @param targetChunkSize Target document count per split (0 to disable adaptive sizing).
   * @param maxSplits Maximum number of splits allowed.
   * @return List of BsonDocument filters.
   */
  public static List<BsonDocument> generateIndexSliceFilters(
      MongoClient client,
      String databaseName,
      String collectionName,
      int numSplits,
      int targetChunkSize,
      int maxSplits) {
    if (client != null) {
      try {
        MongoDatabase db = client.getDatabase(databaseName);
        MongoCollection<BsonDocument> col = db.getCollection(collectionName, BsonDocument.class);
        return generateTypeIsolatedSplits(col, numSplits, targetChunkSize, maxSplits);
      } catch (Exception e) {
        LOG.warn(
            "Failed generating type-isolated splits for '{}.{}' ({})."
                + " Falling back to algorithmic splits.",
            databaseName,
            collectionName,
            e.getMessage());
      }
    }

    int effectiveSplits = Math.max(1, Math.min(maxSplits, numSplits));
    return generateIndexSliceFilters(effectiveSplits);
  }

  /**
   * Generates a list of BsonDocument filter queries for the specified active _id types. If only a
   * single key type is active, no $or wrapper is used.
   *
   * @param numSplits Total target number of parallel read splits.
   * @param activeTypes Set of active IdType values to include.
   * @return List of BsonDocument filters.
   */
  public static List<BsonDocument> generateIndexSliceFilters(
      int numSplits, Set<IdType> activeTypes) {
    if (numSplits <= 1) {
      return Collections.singletonList(new BsonDocument());
    }

    List<BsonDocument> numberFilters =
        activeTypes.contains(IdType.NUMBER)
            ? generateNumberFilters(numSplits)
            : Collections.emptyList();
    List<BsonDocument> stringFilters =
        activeTypes.contains(IdType.STRING)
            ? generateStringFilters(numSplits)
            : Collections.emptyList();
    List<BsonDocument> objectIdFilters =
        activeTypes.contains(IdType.OBJECT_ID)
            ? generateObjectIdFilters(numSplits)
            : Collections.emptyList();

    List<BsonDocument> filters = new ArrayList<>();
    for (int i = 0; i < numSplits; i++) {
      List<BsonDocument> branchFilters = new ArrayList<>();
      if (!numberFilters.isEmpty() && i < numberFilters.size()) {
        branchFilters.add(numberFilters.get(i));
      }
      if (!stringFilters.isEmpty() && i < stringFilters.size()) {
        branchFilters.add(stringFilters.get(i));
      }
      if (!objectIdFilters.isEmpty() && i < objectIdFilters.size()) {
        branchFilters.add(objectIdFilters.get(i));
      }
      if (i == 0 && activeTypes.contains(IdType.OTHER)) {
        branchFilters.add(
            BsonDocument.parse(
                "{\"_id\": {\"$not\": {\"$type\": [\"int\", \"long\", \"double\", \"decimal\","
                    + " \"string\", \"objectId\"]}}}"));
      }

      if (branchFilters.size() == 1) {
        filters.add(branchFilters.get(0));
      } else if (branchFilters.size() > 1) {
        filters.add(new BsonDocument("$or", new BsonArray(branchFilters)));
      }
    }
    if (filters.isEmpty()) {
      filters.add(new BsonDocument());
    }
    return filters;
  }

  private static final BsonArray NUMBER_BSON_TYPES =
      new BsonArray(
          Arrays.asList(
              new BsonString("int"),
              new BsonString("long"),
              new BsonString("double"),
              new BsonString("decimal")));

  public static final class TypeBucket {
    private final String name;
    private final BsonValue typeValue;

    public TypeBucket(String name, BsonValue typeValue) {
      this.name = name;
      this.typeValue = typeValue;
    }

    public String getName() {
      return name;
    }

    public BsonValue getTypeValue() {
      return typeValue;
    }

    public BsonDocument query() {
      return new BsonDocument("_id", new BsonDocument("$type", typeValue));
    }
  }

  private static final List<TypeBucket> KNOWN_TYPE_BUCKETS =
      Collections.unmodifiableList(
          Arrays.asList(
              new TypeBucket("minKey", new BsonString("minKey")),
              new TypeBucket("null", new BsonString("null")),
              new TypeBucket("number", NUMBER_BSON_TYPES),
              new TypeBucket("string", new BsonString("string")),
              new TypeBucket("symbol", new BsonString("symbol")),
              new TypeBucket("object", new BsonString("object")),
              new TypeBucket("binData", new BsonString("binData")),
              new TypeBucket("objectId", new BsonString("objectId")),
              new TypeBucket("bool", new BsonString("bool")),
              new TypeBucket("date", new BsonString("date")),
              new TypeBucket("timestamp", new BsonString("timestamp")),
              new TypeBucket("regex", new BsonString("regex")),
              new TypeBucket("maxKey", new BsonString("maxKey"))));

  public static final class ProbedTypeBounds {
    private final TypeBucket bucket;
    private final BsonValue minKey;
    private final BsonValue maxKey;

    public ProbedTypeBounds(TypeBucket bucket, BsonValue minKey, BsonValue maxKey) {
      this.bucket = bucket;
      this.minKey = minKey;
      this.maxKey = maxKey;
    }

    public TypeBucket getBucket() {
      return bucket;
    }

    public BsonValue getMinKey() {
      return minKey;
    }

    public BsonValue getMaxKey() {
      return maxKey;
    }
  }

  public static TypeBucket getBucketForValue(BsonValue val) {
    if (val == null) {
      return null;
    }
    for (TypeBucket bucket : KNOWN_TYPE_BUCKETS) {
      if (matchesType(val, bucket)) {
        return bucket;
      }
    }
    return null;
  }

  private static String getDbName(MongoCollection<BsonDocument> col) {
    return col != null && col.getNamespace() != null
        ? col.getNamespace().getDatabaseName()
        : "unknown";
  }

  private static String getColName(MongoCollection<BsonDocument> col) {
    return col != null && col.getNamespace() != null
        ? col.getNamespace().getCollectionName()
        : "unknown";
  }

  /**
   * Performs lightweight covered index seeks in O(1) to detect active BSON types and their min/max
   * boundaries. For homogeneous collections (the vast majority), a single min/max check (<2ms)
   * resolves the bounds immediately without probing any other types.
   */
  public static List<ProbedTypeBounds> probeActiveTypeBounds(MongoCollection<BsonDocument> col) {
    try {
      // Step 1: Global Min / Max seek (O(1) covered index scan)
      BsonValue minVal = probeGlobalBound(col, /* direction= */ 1);
      if (minVal == null) {
        return Collections.emptyList();
      }
      BsonValue maxVal = probeGlobalBound(col, /* direction= */ -1);
      if (maxVal == null) {
        return Collections.emptyList();
      }

      TypeBucket minBucket = getBucketForValue(minVal);
      TypeBucket maxBucket = getBucketForValue(maxVal);

      // If either type is unknown, return empty so caller falls back to algorithmic splits
      if (minBucket == null || maxBucket == null) {
        return Collections.emptyList();
      }

      // Case A: Homogeneous collection (min and max have the same BSON type)
      // Because B-tree index stores keys in canonical BSON order, no other type can exist!
      if (minBucket.getName().equals(maxBucket.getName())) {
        return Collections.singletonList(new ProbedTypeBounds(minBucket, minVal, maxVal));
      }

      // Case B: Mixed collection
      // Note: "string" (idx 3) and "symbol" (idx 4) share the same canonical BSON sort rank,
      // so a collection with min=Symbol("a") and max=String("z") has minIdx=4 and maxIdx=3.
      int minIdx = KNOWN_TYPE_BUCKETS.indexOf(minBucket);
      int maxIdx = KNOWN_TYPE_BUCKETS.indexOf(maxBucket);
      boolean isSymbolToStringSharedRank =
          "symbol".equals(minBucket.getName()) && "string".equals(maxBucket.getName());
      if (minIdx < 0 || maxIdx < 0 || (minIdx > maxIdx && !isSymbolToStringSharedRank)) {
        return Collections.emptyList();
      }

      List<ProbedTypeBounds> activeBounds = new ArrayList<>();

      // 1. Min bucket: we already have minVal, seek maxVal for this type
      BsonValue minBucketMax = probeBucketBound(col, minBucket, /* direction= */ -1);
      if (minBucketMax == null) {
        return Collections.emptyList();
      }
      activeBounds.add(new ProbedTypeBounds(minBucket, minVal, minBucketMax));

      // 2. Intermediate candidate buckets: probe existence and min bound in one sorted query
      for (int i = minIdx + 1; i < maxIdx; i++) {
        TypeBucket candidate = KNOWN_TYPE_BUCKETS.get(i);
        BsonValue candMin = probeBucketBound(col, candidate, /* direction= */ 1);
        if (candMin != null) {
          BsonValue candMax = probeBucketBound(col, candidate, /* direction= */ -1);
          if (candMax == null) {
            return Collections.emptyList();
          }
          activeBounds.add(new ProbedTypeBounds(candidate, candMin, candMax));
        }
      }

      // 3. Max bucket: we already have maxVal, seek minVal for this type
      BsonValue maxBucketMin = probeBucketBound(col, maxBucket, /* direction= */ 1);
      if (maxBucketMin == null) {
        return Collections.emptyList();
      }
      activeBounds.add(new ProbedTypeBounds(maxBucket, maxBucketMin, maxVal));

      return activeBounds;
    } catch (Exception e) {
      LOG.warn(
          "Fast index type probe failed on '{}.{}': {}.",
          getDbName(col),
          getColName(col),
          e.getMessage());
      return Collections.emptyList();
    }
  }

  private static BsonValue probeGlobalBound(MongoCollection<BsonDocument> col, int direction) {
    return firstIdFromFind(col.find(), direction);
  }

  private static BsonValue probeBucketBound(
      MongoCollection<BsonDocument> col, TypeBucket bucket, int direction) {
    return firstIdFromFind(col.find(bucket.query()), direction);
  }

  private static BsonValue firstIdFromFind(FindIterable<BsonDocument> iterable, int direction) {
    if (iterable == null) {
      return null;
    }
    BsonDocument doc =
        iterable
            .projection(new BsonDocument("_id", new BsonInt32(1)))
            .sort(new BsonDocument("_id", new BsonInt32(direction)))
            .limit(1)
            .maxTime(5, TimeUnit.SECONDS)
            .first();
    return (doc != null && doc.containsKey("_id")) ? doc.get("_id") : null;
  }

  /**
   * Detects which _id BSON types are present in a MongoDB collection using lightweight index seeks.
   */
  public static Set<IdType> detectIdTypes(
      MongoClient client, String databaseName, String collectionName) {
    EnumSet<IdType> activeTypes = EnumSet.noneOf(IdType.class);
    MongoDatabase db = client.getDatabase(databaseName);
    MongoCollection<BsonDocument> col = db.getCollection(collectionName, BsonDocument.class);

    List<ProbedTypeBounds> bounds = probeActiveTypeBounds(col);
    for (ProbedTypeBounds b : bounds) {
      switch (b.getBucket().getName()) {
        case "number":
          activeTypes.add(IdType.NUMBER);
          break;
        case "string":
          activeTypes.add(IdType.STRING);
          break;
        case "objectId":
          activeTypes.add(IdType.OBJECT_ID);
          break;
        default:
          activeTypes.add(IdType.OTHER);
          break;
      }
    }
    if (activeTypes.isEmpty()) {
      activeTypes.addAll(EnumSet.allOf(IdType.class));
    }
    return activeTypes;
  }

  /**
   * Generates type-isolated splits ensuring filters and keyset resumption never span BSON types.
   */
  public static List<BsonDocument> generateTypeIsolatedSplits(
      MongoCollection<BsonDocument> col, int numSplits) {
    return generateTypeIsolatedSplits(col, numSplits, 0, Math.max(numSplits, 256));
  }

  /**
   * Generates type-isolated splits with adaptive volume sizing, ensuring filters and keyset
   * resumption never span BSON types.
   */
  public static List<BsonDocument> generateTypeIsolatedSplits(
      MongoCollection<BsonDocument> col, int numSplits, int targetChunkSize, int maxSplits) {
    long estimatedDocs = 0;
    try {
      estimatedDocs = col.estimatedDocumentCount();
    } catch (Exception e) {
      LOG.warn(
          "Could not estimate document count for '{}.{}': {}",
          getDbName(col),
          getColName(col),
          e.getMessage());
    }

    if (estimatedDocs > 0 && estimatedDocs <= 5000) {
      return Collections.singletonList(new BsonDocument());
    }

    int effectiveSplits;
    if (targetChunkSize > 0 && estimatedDocs > 0) {
      int calculated = (int) Math.ceil((double) estimatedDocs / targetChunkSize);
      effectiveSplits = Math.max(1, Math.min(maxSplits, calculated));
    } else {
      effectiveSplits = Math.max(1, Math.min(maxSplits, numSplits));
    }

    if (effectiveSplits <= 1) {
      return Collections.singletonList(new BsonDocument());
    }

    List<ProbedTypeBounds> activeTypes = probeActiveTypeBounds(col);
    if (activeTypes.isEmpty()) {
      LOG.info(
          "No active types detected via index probes for '{}.{}'."
              + " Falling back to algorithmic splits.",
          getDbName(col),
          getColName(col));
      return generateIndexSliceFilters(effectiveSplits);
    }

    List<BsonValue> sampledKeys = sampleCollectionKeys(col, effectiveSplits);

    if (activeTypes.size() == 1) {
      return generateSplitsForTypeBounds(col, activeTypes.get(0), effectiveSplits, sampledKeys);
    }

    // Mixed collection: single-key types (minKey == maxKey) and booleans only need 1 split,
    // preserving split budget for multi-document types.
    List<ProbedTypeBounds> multiDocTypes = new ArrayList<>();
    for (ProbedTypeBounds bounds : activeTypes) {
      if (!isSingleSplitBucket(bounds)) {
        multiDocTypes.add(bounds);
      }
    }

    int singleDocCount = activeTypes.size() - multiDocTypes.size();
    int remainingSplits = Math.max(multiDocTypes.size(), effectiveSplits - singleDocCount);

    int totalMultiDocSamples = 0;
    for (ProbedTypeBounds bounds : multiDocTypes) {
      totalMultiDocSamples += countBucketSamples(sampledKeys, bounds.getBucket());
    }

    List<BsonDocument> combinedFilters = new ArrayList<>();
    for (ProbedTypeBounds bounds : activeTypes) {
      if (isSingleSplitBucket(bounds)) {
        combinedFilters.add(bounds.getBucket().query());
        continue;
      }
      int typeSplits;
      if (totalMultiDocSamples > 0) {
        int bucketSamples = countBucketSamples(sampledKeys, bounds.getBucket());
        typeSplits =
            Math.max(
                1,
                (int)
                    Math.round(((double) bucketSamples / totalMultiDocSamples) * remainingSplits));
      } else {
        typeSplits = Math.max(1, remainingSplits / multiDocTypes.size());
      }
      combinedFilters.addAll(generateSplitsForTypeBounds(col, bounds, typeSplits, sampledKeys));
    }
    return combinedFilters;
  }

  private static boolean isSingleSplitBucket(ProbedTypeBounds bounds) {
    return Objects.equals(bounds.getMinKey(), bounds.getMaxKey())
        || "bool".equals(bounds.getBucket().getName());
  }

  private static int countBucketSamples(List<BsonValue> sampledKeys, TypeBucket bucket) {
    int count = 0;
    for (BsonValue k : sampledKeys) {
      if (matchesType(k, bucket)) {
        count++;
      }
    }
    return count;
  }

  private static List<BsonValue> sampleCollectionKeys(
      MongoCollection<BsonDocument> col, int splits) {
    int sampleSize = Math.min(50000, Math.max(1000, splits * 10));
    List<BsonDocument> samplePipeline =
        Arrays.asList(
            new BsonDocument("$sample", new BsonDocument("size", new BsonInt32(sampleSize))),
            new BsonDocument("$project", new BsonDocument("_id", new BsonInt32(1))),
            new BsonDocument("$sort", new BsonDocument("_id", new BsonInt32(1))));

    List<BsonValue> sampledKeys = new ArrayList<>();
    try {
      for (BsonDocument doc : col.aggregate(samplePipeline).maxTime(30, TimeUnit.SECONDS)) {
        if (doc.containsKey("_id")) {
          sampledKeys.add(doc.get("_id"));
        }
      }
    } catch (Exception e) {
      LOG.warn(
          "Unfiltered $sample failed for '{}.{}' ({}). Using probed boundary fallback.",
          getDbName(col),
          getColName(col),
          e.getMessage());
    }
    return sampledKeys;
  }

  private static List<BsonDocument> generateSplitsForTypeBounds(
      ProbedTypeBounds bounds, int splits, List<BsonValue> sampledKeys) {
    return generateSplitsForTypeBounds(null, bounds, splits, sampledKeys);
  }

  private static List<BsonDocument> generateSplitsForTypeBounds(
      MongoCollection<BsonDocument> col,
      ProbedTypeBounds bounds,
      int splits,
      List<BsonValue> sampledKeys) {
    if (splits <= 1 || Objects.equals(bounds.getMinKey(), bounds.getMaxKey())) {
      BsonDocument idDoc = new BsonDocument("$type", bounds.getBucket().getTypeValue());
      return Collections.singletonList(new BsonDocument("_id", idDoc));
    }

    // Non-finite numeric bounds (NaN, +/-Infinity) cannot be partitioned with $gt/$lte range
    // predicates because MongoDB does not match NaN against finite numeric bounds.
    if ("number".equals(bounds.getBucket().getName())
        && (asBigDecimal(bounds.getMinKey()) == null || asBigDecimal(bounds.getMaxKey()) == null)) {
      BsonDocument idDoc = new BsonDocument("$type", bounds.getBucket().getTypeValue());
      return Collections.singletonList(new BsonDocument("_id", idDoc));
    }

    // Filter sampled keys to this specific type (already sorted by MongoDB $sort: {_id: 1})
    List<BsonValue> typeKeys = new ArrayList<>();
    for (BsonValue k : sampledKeys) {
      if (matchesType(k, bounds.getBucket())) {
        typeKeys.add(k);
      }
    }

    if (typeKeys.size() >= 2) {
      List<BsonValue> boundaries = null;
      if ("objectId".equals(bounds.getBucket().getName())
          && bounds.getMinKey() != null
          && bounds.getMaxKey() != null
          && bounds.getMinKey().isObjectId()
          && bounds.getMaxKey().isObjectId()) {
        boundaries =
            generateCounterWeightedObjectIdBoundaries(
                col,
                bounds.getMinKey().asObjectId().getValue().toHexString(),
                bounds.getMaxKey().asObjectId().getValue().toHexString(),
                splits,
                typeKeys);
      }

      if (boundaries == null || boundaries.isEmpty()) {
        int targetSplits = Math.min(splits, typeKeys.size());
        boundaries = new ArrayList<>();
        for (int i = 1; i < targetSplits; i++) {
          int index = (int) Math.round(((double) i * typeKeys.size()) / targetSplits);
          if (index >= typeKeys.size()) {
            index = typeKeys.size() - 1;
          }
          BsonValue b = typeKeys.get(index);
          if (boundaries.isEmpty() || !b.equals(boundaries.get(boundaries.size() - 1))) {
            boundaries.add(b);
          }
        }
      }

      if (!boundaries.isEmpty()) {
        List<BsonDocument> partitions = new ArrayList<>();
        int boundaryCount = boundaries.size();
        for (int i = 0; i <= boundaryCount; i++) {
          BsonDocument filter = new BsonDocument();
          BsonDocument idDoc = new BsonDocument("$type", bounds.getBucket().getTypeValue());
          if (i == 0) {
            idDoc.append("$lte", boundaries.get(0));
          } else if (i == boundaryCount) {
            idDoc.append("$gt", boundaries.get(boundaryCount - 1));
          } else {
            idDoc.append("$gt", boundaries.get(i - 1)).append("$lte", boundaries.get(i));
          }
          filter.append("_id", idDoc);
          partitions.add(filter);
        }
        return partitions;
      }
    }

    // Fallback: If ObjectId, use probed min/max interpolation
    if ("objectId".equals(bounds.getBucket().getName())
        && bounds.getMinKey() != null
        && bounds.getMaxKey() != null
        && bounds.getMinKey().isObjectId()
        && bounds.getMaxKey().isObjectId()) {
      return generateProbedObjectIdSplits(
          bounds.getMinKey().asObjectId().getValue().toHexString(),
          bounds.getMaxKey().asObjectId().getValue().toHexString(),
          splits);
    }

    // Fallback: If Number, interpolate probed [minKey, maxKey] range (or use $mod if unprobed)
    if ("number".equals(bounds.getBucket().getName())) {
      if (isNumeric(bounds.getMinKey()) && isNumeric(bounds.getMaxKey())) {
        return generateProbedNumberSplits(bounds.getMinKey(), bounds.getMaxKey(), splits);
      }
      return generateNumberFilters(splits);
    }

    // Fallback: If String, use string bounds
    if ("string".equals(bounds.getBucket().getName())) {
      return generateStringFilters(splits);
    }

    BsonDocument fallbackId = new BsonDocument("$type", bounds.getBucket().getTypeValue());
    return Collections.singletonList(new BsonDocument("_id", fallbackId));
  }

  /**
   * Eliminates WiredTiger {@code $sample} B-tree depth skew on {@link ObjectId} collections by
   * weighting adjacent sampled intervals {@code [k_i, k_{i+1}]} using the 24-bit monotonic {@link
   * ObjectId} counter delta {@code (ctr(k_{i+1}) - ctr(k_i)) & 0xFFFFFF} and interpolating within
   * wide intervals while preserving the 5-byte {@code processUnique} identifier.
   */
  static List<BsonValue> generateCounterWeightedObjectIdBoundaries(
      MongoCollection<BsonDocument> col,
      String minHex,
      String maxHex,
      int splits,
      List<BsonValue> typeKeys) {
    if (minHex == null
        || maxHex == null
        || minHex.length() != 24
        || maxHex.length() != 24
        || minHex.compareToIgnoreCase(maxHex) >= 0) {
      return Collections.emptyList();
    }
    String lowerMin = minHex.toLowerCase();
    String lowerMax = maxHex.toLowerCase();
    String procHex = lowerMin.substring(8, 18);
    if (!procHex.equals(lowerMax.substring(8, 18))) {
      return Collections.emptyList();
    }

    List<String> hexKeys = new ArrayList<>(typeKeys.size() + 2);
    hexKeys.add(lowerMin);
    int matchingProcCount = 0;
    int totalOidSamples = 0;
    for (BsonValue val : typeKeys) {
      if (val != null && val.isObjectId()) {
        totalOidSamples++;
        String h = val.asObjectId().getValue().toHexString().toLowerCase();
        if (procHex.equals(h.substring(8, 18))) {
          matchingProcCount++;
          if (h.compareTo(hexKeys.get(hexKeys.size() - 1)) > 0 && h.compareTo(lowerMax) < 0) {
            hexKeys.add(h);
          }
        }
      }
    }
    hexKeys.add(lowerMax);

    if (totalOidSamples > 0 && matchingProcCount * 5 < totalOidSamples * 4) {
      return Collections.emptyList();
    }

    // Refine any wide multi-second gaps (such as the top B-tree tail missed by $sample) using
    // lightweight O(1) index seeks at 1-second timestamp boundaries (capped at 64 seeks).
    if (col != null) {
      long initialSpan = 0L;
      for (int i = 0; i < hexKeys.size() - 1; i++) {
        initialSpan +=
            Math.max(
                1L,
                (parseOidCounter(hexKeys.get(i + 1)) - parseOidCounter(hexKeys.get(i)))
                    & 0xFFFFFFL);
      }
      long gapThreshold = Math.max(1000L, initialSpan / Math.max(2L, splits * 2L));
      int remainingProbes = 64;
      List<String> refined = new ArrayList<>(hexKeys.size() + 16);
      for (int i = 0; i < hexKeys.size() - 1; i++) {
        String cur = hexKeys.get(i);
        String next = hexKeys.get(i + 1);
        refined.add(cur);
        long curTs = parseOidTimestamp(cur);
        long nextTs = parseOidTimestamp(next);
        long delta = (parseOidCounter(next) - parseOidCounter(cur)) & 0xFFFFFFL;
        if (nextTs > curTs && delta > gapThreshold && remainingProbes > 0) {
          long secSpan = nextTs - curTs;
          long stepSec = Math.max(1L, (secSpan + remainingProbes - 1L) / remainingProbes);
          String prevInserted = cur;
          for (long ts = curTs; ts < nextTs && remainingProbes > 0; ts += stepSec) {
            remainingProbes--;
            String probed = probeMaxObjectIdInSecond(col, ts);
            if (probed != null
                && procHex.equals(probed.substring(8, 18))
                && probed.compareTo(prevInserted) > 0
                && probed.compareTo(next) < 0) {
              refined.add(probed);
              prevInserted = probed;
            }
          }
        }
      }
      refined.add(lowerMax);
      hexKeys = refined;
    }

    int n = hexKeys.size();
    long[] cumulativeWeight = new long[n];
    for (int i = 0; i < n - 1; i++) {
      long delta =
          (parseOidCounter(hexKeys.get(i + 1)) - parseOidCounter(hexKeys.get(i))) & 0xFFFFFFL;
      cumulativeWeight[i + 1] = cumulativeWeight[i] + Math.max(1L, delta);
    }
    long totalWeight = cumulativeWeight[n - 1];
    if (totalWeight < splits) {
      return Collections.emptyList();
    }

    List<BsonValue> boundaries = new ArrayList<>(splits - 1);
    String prevBoundaryHex = lowerMin;
    int intervalIdx = 0;
    for (int s = 1; s < splits; s++) {
      long targetW = Math.round(((double) s * totalWeight) / splits);
      while (intervalIdx < n - 2 && cumulativeWeight[intervalIdx + 1] < targetW) {
        intervalIdx++;
      }
      String lowHex = hexKeys.get(intervalIdx);
      String highHex = hexKeys.get(intervalIdx + 1);
      long offset = Math.max(0L, targetW - cumulativeWeight[intervalIdx]);
      long span = Math.max(1L, cumulativeWeight[intervalIdx + 1] - cumulativeWeight[intervalIdx]);

      long lowTs = parseOidTimestamp(lowHex);
      long highTs = parseOidTimestamp(highHex);
      long lowCtr = parseOidCounter(lowHex);
      long interpCtr = (lowCtr + offset) & 0xFFFFFFL;
      long interpTs =
          (lowTs == highTs)
              ? lowTs
              : lowTs + Math.round(((double) offset / span) * (highTs - lowTs));

      String candHex = String.format("%08x%s%06x", interpTs, procHex, interpCtr);
      if (candHex.compareTo(lowHex) < 0 || candHex.compareTo(highHex) > 0) {
        candHex = (offset * 2 < span) ? lowHex : highHex;
      }
      if (candHex.compareTo(prevBoundaryHex) > 0 && candHex.compareTo(lowerMax) < 0) {
        boundaries.add(new BsonObjectId(new ObjectId(candHex)));
        prevBoundaryHex = candHex;
      }
    }
    return boundaries;
  }

  private static long parseOidTimestamp(String hex24) {
    return Long.parseLong(hex24.substring(0, 8), 16);
  }

  private static long parseOidCounter(String hex24) {
    return Long.parseLong(hex24.substring(18, 24), 16);
  }

  private static String probeMaxObjectIdInSecond(MongoCollection<BsonDocument> col, long epochSec) {
    try {
      String tsHex = String.format("%08x", epochSec);
      BsonDocument range =
          new BsonDocument(
              "_id",
              new BsonDocument("$gte", new BsonObjectId(new ObjectId(tsHex + "0000000000000000")))
                  .append("$lte", new BsonObjectId(new ObjectId(tsHex + "ffffffffffffffff"))));
      BsonValue val = firstIdFromFind(col.find(range), /* direction= */ -1);
      if (val != null && val.isObjectId()) {
        return val.asObjectId().getValue().toHexString().toLowerCase();
      }
    } catch (Exception ignored) {
    }
    return null;
  }

  private static boolean matchesType(BsonValue k, TypeBucket bucket) {
    if (k == null) {
      return false;
    }
    switch (bucket.getName()) {
      case "minKey":
        return k.getBsonType() == BsonType.MIN_KEY;
      case "null":
        return k.isNull();
      case "number":
        return k.isInt32() || k.isInt64() || k.isDouble() || k.isDecimal128();
      case "string":
        return k.isString();
      case "symbol":
        return k.isSymbol();
      case "object":
        return k.isDocument();
      case "binData":
        return k.isBinary();
      case "objectId":
        return k.isObjectId();
      case "bool":
        return k.isBoolean();
      case "date":
        return k.isDateTime();
      case "timestamp":
        return k.isTimestamp();
      case "regex":
        return k.isRegularExpression();
      case "maxKey":
        return k.getBsonType() == BsonType.MAX_KEY;
      default:
        return false;
    }
  }

  private static boolean isNumeric(BsonValue val) {
    return val != null && (val.isInt32() || val.isInt64() || val.isDouble() || val.isDecimal128());
  }

  private static BigDecimal asBigDecimal(BsonValue val) {
    if (val == null) {
      return null;
    }
    if (val.isInt32()) {
      return BigDecimal.valueOf(val.asInt32().getValue());
    }
    if (val.isInt64()) {
      return BigDecimal.valueOf(val.asInt64().getValue());
    }
    if (val.isDouble()) {
      double d = val.asDouble().getValue();
      if (Double.isNaN(d) || Double.isInfinite(d)) {
        return null;
      }
      return BigDecimal.valueOf(d);
    }
    if (val.isDecimal128()) {
      Decimal128 dec = val.asDecimal128().getValue();
      if (!dec.isFinite()) {
        return null;
      }
      try {
        return dec.bigDecimalValue();
      } catch (ArithmeticException e) {
        // Finite Decimal128 only throws ArithmeticException for negative zero (-0)
        return BigDecimal.ZERO;
      }
    }
    return null;
  }

  private static BsonValue toBsonNumber(
      BigDecimal val, BsonValue originalMin, BsonValue originalMax) {
    if (originalMin.isDecimal128() || originalMax.isDecimal128()) {
      return new BsonDecimal128(new Decimal128(val.round(MathContext.DECIMAL128)));
    }
    if (originalMin.isDouble() || originalMax.isDouble()) {
      return new BsonDouble(val.doubleValue());
    }
    return new BsonInt64(val.longValue());
  }

  /** Generates contiguous numeric range splits interpolated between probed min and max values. */
  public static List<BsonDocument> generateProbedNumberSplits(
      BsonValue minVal, BsonValue maxVal, int numSplits) {
    BigDecimal minDec = asBigDecimal(minVal);
    BigDecimal maxDec = asBigDecimal(maxVal);
    if (numSplits <= 1 || minDec == null || maxDec == null) {
      return singleNumberBucketFilter();
    }
    BigDecimal range = maxDec.subtract(minDec);
    if (range.compareTo(BigDecimal.ZERO) <= 0) {
      return singleNumberBucketFilter();
    }

    boolean isFloatingPoint =
        minVal.isDecimal128() || maxVal.isDecimal128() || minVal.isDouble() || maxVal.isDouble();
    BigDecimal step =
        isFloatingPoint
            ? range.divide(BigDecimal.valueOf(numSplits), MathContext.DECIMAL128)
            : range.divideToIntegralValue(BigDecimal.valueOf(numSplits));
    if (step.compareTo(BigDecimal.ZERO) <= 0) {
      return singleNumberBucketFilter();
    }

    List<BsonValue> boundaries = new ArrayList<>(numSplits - 1);
    for (int i = 1; i < numSplits; i++) {
      BsonValue bound =
          toBsonNumber(minDec.add(step.multiply(BigDecimal.valueOf(i))), minVal, maxVal);
      if (boundaries.isEmpty() || !bound.equals(boundaries.get(boundaries.size() - 1))) {
        boundaries.add(bound);
      }
    }
    if (boundaries.isEmpty()) {
      return singleNumberBucketFilter();
    }

    List<BsonDocument> slices = new ArrayList<>(boundaries.size() + 1);
    for (int i = 0; i <= boundaries.size(); i++) {
      BsonDocument idDoc = new BsonDocument("$type", NUMBER_BSON_TYPES);
      if (i == 0) {
        idDoc.append("$lt", boundaries.get(0));
      } else if (i == boundaries.size()) {
        idDoc.append("$gte", boundaries.get(boundaries.size() - 1));
      } else {
        idDoc.append("$gte", boundaries.get(i - 1)).append("$lt", boundaries.get(i));
      }
      slices.add(new BsonDocument("_id", idDoc));
    }
    return slices;
  }

  private static List<BsonDocument> singleNumberBucketFilter() {
    return Collections.singletonList(
        new BsonDocument("_id", new BsonDocument("$type", NUMBER_BSON_TYPES)));
  }

  /**
   * Generates contiguous, uniform ObjectId splits interpolated between actual probed min/max hex
   * keys.
   */
  public static List<BsonDocument> generateProbedObjectIdSplits(
      String minHex, String maxHex, int numSplits) {
    if (numSplits <= 1 || minHex.equals(maxHex)) {
      return Collections.singletonList(BsonDocument.parse("{\"_id\": {\"$type\": \"objectId\"}}"));
    }

    BigInteger minBig = new BigInteger(minHex, 16);
    BigInteger maxBig = new BigInteger(maxHex, 16);
    BigInteger range = maxBig.subtract(minBig);
    BigInteger step = range.divide(BigInteger.valueOf(numSplits));

    if (step.compareTo(BigInteger.ZERO) <= 0) {
      return Collections.singletonList(BsonDocument.parse("{\"_id\": {\"$type\": \"objectId\"}}"));
    }

    List<BsonDocument> slices = new ArrayList<>();
    for (int i = 0; i < numSplits; i++) {
      if (i == 0) {
        BigInteger high = minBig.add(step);
        String highHex = String.format("%024x", high);
        slices.add(
            BsonDocument.parse(
                String.format(
                    "{\"_id\": {\"$type\": \"objectId\", \"$lt\": {\"$oid\": \"%s\"}}}", highHex)));
      } else if (i == numSplits - 1) {
        BigInteger low = minBig.add(step.multiply(BigInteger.valueOf(i)));
        String lowHex = String.format("%024x", low);
        slices.add(
            BsonDocument.parse(
                String.format(
                    "{\"_id\": {\"$type\": \"objectId\", \"$gte\": {\"$oid\": \"%s\"}}}", lowHex)));
      } else {
        BigInteger low = minBig.add(step.multiply(BigInteger.valueOf(i)));
        BigInteger high = minBig.add(step.multiply(BigInteger.valueOf(i + 1)));
        String lowHex = String.format("%024x", low);
        String highHex = String.format("%024x", high);
        slices.add(
            BsonDocument.parse(
                String.format(
                    "{\"_id\": {\"$type\": \"objectId\", \"$gte\": {\"$oid\": \"%s\"}, \"$lt\":"
                        + " {\"$oid\": \"%s\"}}}",
                    lowHex, highHex)));
      }
    }
    return slices;
  }

  private static List<BsonDocument> generateNumberFilters(int numSplits) {
    List<BsonDocument> filters = new ArrayList<>();
    for (int r = 0; r < numSplits; r++) {
      BsonDocument filter =
          BsonDocument.parse(
              String.format(
                  "{\"_id\": {\"$type\": [\"int\", \"long\", \"double\", \"decimal\"], \"$mod\":"
                      + " [%d, %d]}}",
                  numSplits, r));
      filters.add(filter);
    }
    return filters;
  }

  private static List<BsonDocument> generateStringFilters(int numSplits) {
    List<BsonDocument> filters = new ArrayList<>();
    List<String> stringBounds = generateStringBounds(numSplits);
    for (int i = 0; i < stringBounds.size() - 1; i++) {
      String low = stringBounds.get(i);
      String high = stringBounds.get(i + 1);
      String lowClause = low.isEmpty() ? "" : String.format(", \"$gte\": \"%s\"", low);
      String highClause =
          (i == stringBounds.size() - 2) ? "" : String.format(", \"$lt\": \"%s\"", high);
      BsonDocument filter =
          BsonDocument.parse(
              String.format("{\"_id\": {\"$type\": \"string\"%s%s}}", lowClause, highClause));
      filters.add(filter);
    }
    return filters;
  }

  private static List<BsonDocument> generateObjectIdFilters(int numSplits) {
    List<BsonDocument> filters = new ArrayList<>();
    List<String> hexBounds = generateObjectIdBounds(numSplits);
    for (int i = 0; i < hexBounds.size() - 1; i++) {
      String lowHex = hexBounds.get(i);
      String highHex = hexBounds.get(i + 1);
      String highOp = (i == hexBounds.size() - 2) ? "$lte" : "$lt";
      BsonDocument filter =
          BsonDocument.parse(
              String.format(
                  "{\"_id\": {\"$type\": \"objectId\", \"$gte\": {\"$oid\": \"%s\"}, \"%s\":"
                      + " {\"$oid\": \"%s\"}}}",
                  lowHex, highOp, highHex));
      filters.add(filter);
    }
    return filters;
  }

  private static final String STRING_SPLIT_CHARS =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  private static List<String> generateStringBounds(int numSplits) {
    List<String> bounds = new ArrayList<>();
    bounds.add("");
    if (numSplits == 1) {
      bounds.add("\uffff");
      return bounds;
    }
    int maxIndex = STRING_SPLIT_CHARS.length() - 1;
    int step = Math.max(1, maxIndex / numSplits);
    for (int i = 1; i < numSplits; i++) {
      int idx = Math.min(maxIndex, i * step);
      String candidate = String.valueOf(STRING_SPLIT_CHARS.charAt(idx));
      if (!candidate.equals(bounds.get(bounds.size() - 1))) {
        bounds.add(candidate);
      }
    }
    bounds.add("\uffff");
    return bounds;
  }

  private static List<String> generateObjectIdBounds(int numSplits) {
    List<String> bounds = new ArrayList<>();
    long nowSec = System.currentTimeMillis() / 1000L;
    long minSec = Math.max(0L, nowSec - (5L * 365 * 86400));
    long maxSec = nowSec + (30L * 86400);
    long step = (maxSec - minSec) / numSplits;
    for (int i = 0; i <= numSplits; i++) {
      if (i == 0) {
        bounds.add("000000000000000000000000");
      } else if (i == numSplits) {
        bounds.add("ffffffffffffffffffffffff");
      } else {
        long val = minSec + i * step;
        String hexPrefix = String.format("%08x", val);
        bounds.add(hexPrefix + "0000000000000000");
      }
    }
    return bounds;
  }
}
