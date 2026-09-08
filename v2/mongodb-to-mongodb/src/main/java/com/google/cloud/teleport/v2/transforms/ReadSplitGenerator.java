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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
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
   * Generates a list of BsonDocument filter queries using 2-phase covered index type discovery
   * and type-isolated quantile sampling.
   *
   * @param client MongoDB client connection.
   * @param databaseName Database name.
   * @param collectionName Collection name.
   * @param numSplits Number of target parallel read splits.
   * @return List of BsonDocument filters.
   */
  public static List<BsonDocument> generateIndexSliceFilters(
      MongoClient client, String databaseName, String collectionName, int numSplits) {
    if (numSplits <= 1) {
      return Collections.singletonList(new BsonDocument());
    }

    if (client != null) {
      try {
        MongoDatabase db = client.getDatabase(databaseName);
        MongoCollection<BsonDocument> col = db.getCollection(collectionName, BsonDocument.class);
        return generateTypeIsolatedSplits(col, numSplits);
      } catch (Exception e) {
        LOG.warn(
            "Failed generating type-isolated splits for '{}.{}' ({})."
                + " Falling back to algorithmic splits.",
            databaseName,
            collectionName,
            e.getMessage());
      }
    }

    return generateIndexSliceFilters(numSplits);
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

      if (branchFilters.isEmpty()) {
        filters.add(new BsonDocument());
      } else if (branchFilters.size() == 1) {
        filters.add(branchFilters.get(0));
      } else {
        filters.add(new BsonDocument("$or", new BsonArray(branchFilters)));
      }
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
              new TypeBucket("number", NUMBER_BSON_TYPES),
              new TypeBucket("string", new BsonString("string")),
              new TypeBucket("objectId", new BsonString("objectId")),
              new TypeBucket("binData", new BsonString("binData")),
              new TypeBucket("object", new BsonString("object")),
              new TypeBucket("date", new BsonString("date"))));

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
   * Performs lightweight covered index seeks (<26ms) to detect all active BSON types and their
   * min/max boundaries.
   */
  public static List<ProbedTypeBounds> probeActiveTypeBounds(MongoCollection<BsonDocument> col) {
    List<ProbedTypeBounds> activeBounds = new ArrayList<>();
    for (TypeBucket bucket : KNOWN_TYPE_BUCKETS) {
      try {
        BsonDocument minDoc =
            col.find(bucket.query())
                .projection(new BsonDocument("_id", new BsonInt32(1)))
                .sort(new BsonDocument("_id", new BsonInt32(1)))
                .limit(1)
                .maxTime(3, TimeUnit.SECONDS)
                .first();
        if (minDoc != null && minDoc.containsKey("_id")) {
          BsonDocument maxDoc =
              col.find(bucket.query())
                  .projection(new BsonDocument("_id", new BsonInt32(1)))
                  .sort(new BsonDocument("_id", new BsonInt32(-1)))
                  .limit(1)
                  .maxTime(3, TimeUnit.SECONDS)
                  .first();
          BsonValue minVal = minDoc.get("_id");
          BsonValue maxVal =
              (maxDoc != null && maxDoc.containsKey("_id")) ? maxDoc.get("_id") : minVal;
          activeBounds.add(new ProbedTypeBounds(bucket, minVal, maxVal));
        }
      } catch (Exception e) {
        LOG.warn(
            "Covered index probe failed for type '{}' on '{}.{}': {}",
            bucket.getName(),
            getDbName(col),
            getColName(col),
            e.getMessage());
      }
    }
    return activeBounds;
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
    if (numSplits <= 1) {
      return Collections.singletonList(new BsonDocument());
    }

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

    List<ProbedTypeBounds> activeTypes = probeActiveTypeBounds(col);
    if (activeTypes.isEmpty()) {
      LOG.info(
          "No active types detected via index probes for '{}.{}'."
              + " Falling back to algorithmic splits.",
          getDbName(col),
          getColName(col));
      return generateIndexSliceFilters(numSplits);
    }

    if (activeTypes.size() == 1) {
      return generateSplitsForTypeBounds(col, activeTypes.get(0), numSplits);
    }

    // Mixed collection: allocate splits across active types without cross-type queries
    int splitsPerType = Math.max(1, numSplits / activeTypes.size());
    List<BsonDocument> combinedFilters = new ArrayList<>();
    for (ProbedTypeBounds bounds : activeTypes) {
      combinedFilters.addAll(generateSplitsForTypeBounds(col, bounds, splitsPerType));
    }
    return combinedFilters;
  }

  private static List<BsonDocument> generateSplitsForTypeBounds(
      MongoCollection<BsonDocument> col, ProbedTypeBounds bounds, int splits) {
    if (splits <= 1) {
      return Collections.singletonList(
          new BsonDocument("_id", new BsonDocument("$type", bounds.getBucket().getTypeValue())));
    }

    // Try fast unfiltered $sample for quantile boundaries
    int sampleSize = Math.min(1000, Math.max(256, splits * 32));
    List<BsonDocument> samplePipeline =
        Arrays.asList(
            new BsonDocument("$sample", new BsonDocument("size", new BsonInt32(sampleSize))),
            new BsonDocument("$project", new BsonDocument("_id", new BsonInt32(1))),
            new BsonDocument("$sort", new BsonDocument("_id", new BsonInt32(1))));

    List<BsonValue> sampledKeys = new ArrayList<>();
    try {
      for (BsonDocument doc : col.aggregate(samplePipeline).maxTime(5, TimeUnit.SECONDS)) {
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

    // Filter sampled keys to this specific type
    List<BsonValue> typeKeys = new ArrayList<>();
    for (BsonValue k : sampledKeys) {
      if (matchesType(k, bounds.getBucket())) {
        typeKeys.add(k);
      }
    }

    if (typeKeys.size() >= splits) {
      typeKeys.sort(BSON_VALUE_COMPARATOR);
      List<BsonValue> boundaries = new ArrayList<>();
      int step = typeKeys.size() / splits;
      for (int i = 1; i < splits; i++) {
        BsonValue b = typeKeys.get(i * step);
        if (boundaries.isEmpty() || !b.equals(boundaries.get(boundaries.size() - 1))) {
          boundaries.add(b);
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
        && bounds.getMinKey().isObjectId()
        && bounds.getMaxKey().isObjectId()) {
      return generateProbedObjectIdSplits(
          bounds.getMinKey().asObjectId().getValue().toHexString(),
          bounds.getMaxKey().asObjectId().getValue().toHexString(),
          splits);
    }

    // Fallback: If Number, use $mod
    if ("number".equals(bounds.getBucket().getName())) {
      return generateNumberFilters(splits);
    }

    // Fallback: If String, use string bounds
    if ("string".equals(bounds.getBucket().getName())) {
      return generateStringFilters(splits);
    }

    return Collections.singletonList(
        new BsonDocument("_id", new BsonDocument("$type", bounds.getBucket().getTypeValue())));
  }

  private static boolean matchesType(BsonValue k, TypeBucket bucket) {
    if (k == null) {
      return false;
    }
    switch (bucket.getName()) {
      case "number":
        return k.isInt32() || k.isInt64() || k.isDouble() || k.isDecimal128();
      case "string":
        return k.isString();
      case "objectId":
        return k.isObjectId();
      case "binData":
        return k.isBinary();
      case "object":
        return k.isDocument();
      case "date":
        return k.isDateTime();
      default:
        return false;
    }
  }

  private static final Comparator<BsonValue> BSON_VALUE_COMPARATOR =
      (a, b) -> {
        if (a == b) {
          return 0;
        }
        if (a == null) {
          return -1;
        }
        if (b == null) {
          return 1;
        }
        if (a.isObjectId() && b.isObjectId()) {
          return a.asObjectId().compareTo(b.asObjectId());
        }
        if (a.isString() && b.isString()) {
          return a.asString().getValue().compareTo(b.asString().getValue());
        }
        if (isNumeric(a) && isNumeric(b)) {
          return Double.compare(asDouble(a), asDouble(b));
        }
        if (a.isDateTime() && b.isDateTime()) {
          return Long.compare(a.asDateTime().getValue(), b.asDateTime().getValue());
        }
        if (a.isBinary() && b.isBinary()) {
          return Arrays.compare(a.asBinary().getData(), b.asBinary().getData());
        }
        return a.toString().compareTo(b.toString());
      };

  private static boolean isNumeric(BsonValue val) {
    return val != null
        && (val.isInt32() || val.isInt64() || val.isDouble() || val.isDecimal128());
  }

  private static double asDouble(BsonValue val) {
    if (val.isInt32()) {
      return val.asInt32().getValue();
    }
    if (val.isInt64()) {
      return val.asInt64().getValue();
    }
    if (val.isDouble()) {
      return val.asDouble().getValue();
    }
    if (val.isDecimal128()) {
      return val.asDecimal128().getValue().doubleValue();
    }
    return 0.0;
  }

  /**
   * Generates contiguous, uniform ObjectId splits interpolated between actual probed min/max
   * hex keys.
   */
  public static List<BsonDocument> generateProbedObjectIdSplits(
      String minHex, String maxHex, int numSplits) {
    if (numSplits <= 1 || minHex.equals(maxHex)) {
      return Collections.singletonList(
          new BsonDocument("_id", new BsonDocument("$type", new BsonString("objectId"))));
    }

    BigInteger minBig = new BigInteger(minHex, 16);
    BigInteger maxBig = new BigInteger(maxHex, 16);
    BigInteger range = maxBig.subtract(minBig);
    BigInteger step = range.divide(BigInteger.valueOf(numSplits));

    if (step.compareTo(BigInteger.ZERO) <= 0) {
      return Collections.singletonList(
          new BsonDocument("_id", new BsonDocument("$type", new BsonString("objectId"))));
    }

    List<BsonDocument> slices = new ArrayList<>();
    for (int i = 0; i < numSplits; i++) {
      if (i == 0) {
        BigInteger high = minBig.add(step);
        String highHex = String.format("%024x", high);
        slices.add(
            BsonDocument.parse(
                String.format(
                    "{\"_id\": {\"$type\": \"objectId\", \"$lt\": {\"$oid\": \"%s\"}}}",
                    highHex)));
      } else if (i == numSplits - 1) {
        BigInteger low = minBig.add(step.multiply(BigInteger.valueOf(i)));
        String lowHex = String.format("%024x", low);
        slices.add(
            BsonDocument.parse(
                String.format(
                    "{\"_id\": {\"$type\": \"objectId\", \"$gte\": {\"$oid\": \"%s\"}}}",
                    lowHex)));
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
          (i == stringBounds.size() - 2)
              ? String.format(", \"$lte\": \"%s\"", high)
              : String.format(", \"$lt\": \"%s\"", high);
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
      bounds.add(String.valueOf(STRING_SPLIT_CHARS.charAt(idx)));
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
