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
package com.google.cloud.teleport.v2.mongodb.validator.planner;

import com.google.cloud.teleport.v2.mongodb.validator.comparison.BsonOrderComparator;
import com.google.cloud.teleport.v2.mongodb.validator.dto.MismatchType;
import com.google.cloud.teleport.v2.mongodb.validator.dto.MismatchedDocument;
import com.google.cloud.teleport.v2.mongodb.validator.dto.ValidationSlice;
import com.google.cloud.teleport.v2.mongodb.validator.dto.ValidationSlice.SliceMode;
import com.google.cloud.teleport.v2.mongodb.validator.util.MongoClientManager;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.Serializable;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discovers the union of source and target collections, audits secondary index/TTL parity, and
 * partitions matched collection pairs into disjoint, type-isolated {@link ValidationSlice}s
 * covering {@code (-inf, +inf)}.
 */
public final class CollectionAndIndexPlanner {

  private static final Logger LOG = LoggerFactory.getLogger(CollectionAndIndexPlanner.class);

  private static final JsonWriterSettings EXTENDED_JSON =
      JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

  private static final BsonArray NUMBER_TYPES =
      new BsonArray(
          Arrays.asList(
              new BsonString("int"),
              new BsonString("long"),
              new BsonString("double"),
              new BsonString("decimal")));

  private static final String OTHER_TYPES_FILTER_JSON =
      "{\"_id\": {\"$not\": {\"$type\": [\"int\", \"long\", \"double\", \"decimal\","
          + " \"string\", \"objectId\"]}}}";

  /** Bounds for a probed scalar {@code _id} type bucket on one collection. */
  public static final class TypeBounds {
    private final String typeName;
    private final BsonValue minId;
    private final BsonValue maxId;
    private final long estimatedCount;

    public TypeBounds(String typeName, BsonValue minId, BsonValue maxId, long estimatedCount) {
      this.typeName = Objects.requireNonNull(typeName, "typeName");
      this.minId = Objects.requireNonNull(minId, "minId");
      this.maxId = Objects.requireNonNull(maxId, "maxId");
      this.estimatedCount = Math.max(1L, estimatedCount);
    }

    public String getTypeName() {
      return typeName;
    }

    public BsonValue getMinId() {
      return minId;
    }

    public BsonValue getMaxId() {
      return maxId;
    }

    public long getEstimatedCount() {
      return estimatedCount;
    }
  }

  /** Normalized secondary index specification for cross-engine comparison. */
  public static final class NormalizedIndexSpec implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final String canonicalKeySpec;
    private final boolean unique;
    private final boolean sparse;
    @Nullable private final Long expireAfterSeconds;

    public NormalizedIndexSpec(
        String name,
        String canonicalKeySpec,
        boolean unique,
        boolean sparse,
        @Nullable Long expireAfterSeconds) {
      this.name = name;
      this.canonicalKeySpec = canonicalKeySpec;
      this.unique = unique;
      this.sparse = sparse;
      this.expireAfterSeconds = expireAfterSeconds;
    }

    public String getName() {
      return name;
    }

    public String getCanonicalKeySpec() {
      return canonicalKeySpec;
    }

    public boolean isUnique() {
      return unique;
    }

    public boolean isSparse() {
      return sparse;
    }

    @Nullable
    public Long getExpireAfterSeconds() {
      return expireAfterSeconds;
    }

    public String describeOptions() {
      return String.format(
          "unique=%s, sparse=%s, expireAfterSeconds=%s", unique, sparse, expireAfterSeconds);
    }

    public boolean optionsMatch(NormalizedIndexSpec other) {
      return unique == other.unique
          && sparse == other.sparse
          && Objects.equals(expireAfterSeconds, other.expireAfterSeconds);
    }
  }

  /** Seam abstracting catalog, index, and type-bound metadata queries for unit testing. */
  public interface MetadataInspector {
    Set<String> listUserCollections(boolean isTarget);

    List<NormalizedIndexSpec> listSecondaryIndexes(boolean isTarget, String collection);

    long estimatedDocumentCount(boolean isTarget, String collection);

    @Nullable
    TypeBounds probeTypeBounds(boolean isTarget, String collection, String typeName);

    boolean hasOtherTypeDocuments(boolean isTarget, String collection);
  }

  /** Complete output of pre-execution planning. */
  public static final class PlanResult {
    private final List<ValidationSlice> slices;
    private final List<MismatchedDocument> catalogAndIndexMismatches;
    private final Map<String, String> allCollectionPairs;

    public PlanResult(
        List<ValidationSlice> slices,
        List<MismatchedDocument> catalogAndIndexMismatches,
        Map<String, String> allCollectionPairs) {
      this.slices = ImmutableList.copyOf(slices);
      this.catalogAndIndexMismatches = ImmutableList.copyOf(catalogAndIndexMismatches);
      this.allCollectionPairs =
          Collections.unmodifiableMap(new LinkedHashMap<>(allCollectionPairs));
    }

    public List<ValidationSlice> getSlices() {
      return slices;
    }

    public List<MismatchedDocument> getCatalogAndIndexMismatches() {
      return catalogAndIndexMismatches;
    }

    public Map<String, String> getAllCollectionPairs() {
      return allCollectionPairs;
    }
  }

  private CollectionAndIndexPlanner() {}

  /** Builds a live {@link PlanResult} against source MongoDB and target Firestore URIs. */
  public static PlanResult planLive(
      String sourceUri,
      String targetUri,
      String sourceDb,
      String targetDb,
      String collectionsCsv,
      String collectionMappingsCsv,
      boolean validateIndexes,
      int targetChunkSize,
      int maxSplitsPerCollection,
      String runId,
      Instant planInstant) {
    try (MongoClient srcClient = MongoClientManager.createDedicated(sourceUri);
        MongoClient tgtClient = MongoClientManager.createDedicated(targetUri)) {
      MongoDatabase sDb = srcClient.getDatabase(sourceDb);
      MongoDatabase tDb = tgtClient.getDatabase(targetDb);
      MetadataInspector inspector = new LiveMongoMetadataInspector(sDb, tDb);
      return buildPlan(
          inspector,
          sourceDb,
          targetDb,
          collectionsCsv,
          collectionMappingsCsv,
          validateIndexes,
          targetChunkSize,
          maxSplitsPerCollection,
          runId,
          planInstant);
    }
  }

  /** Builds a deterministic {@link PlanResult} using the supplied {@link MetadataInspector}. */
  public static PlanResult buildPlan(
      MetadataInspector inspector,
      String sourceDb,
      String targetDb,
      String collectionsCsv,
      String collectionMappingsCsv,
      boolean validateIndexes,
      int targetChunkSize,
      int maxSplitsPerCollection,
      String runId,
      Instant planInstant) {
    Map<String, String> forwardMap = parseCollectionMappings(collectionMappingsCsv);
    Map<String, String> reverseMap = new LinkedHashMap<>();
    for (Map.Entry<String, String> e : forwardMap.entrySet()) {
      reverseMap.put(e.getValue(), e.getKey());
    }

    Set<String> srcExisting = inspector.listUserCollections(false);
    Set<String> tgtExisting = inspector.listUserCollections(true);
    Set<String> explicitFilter = parseCsvSet(collectionsCsv);

    Map<String, String> allPairs = new LinkedHashMap<>();
    List<ValidationSlice> slices = new ArrayList<>();
    List<MismatchedDocument> mismatches = new ArrayList<>();
    String nowIso = planInstant.toString();

    if (!explicitFilter.isEmpty()) {
      for (String srcCol : explicitFilter) {
        String tgtCol = forwardMap.getOrDefault(srcCol, srcCol);
        allPairs.put(srcCol, tgtCol);
        boolean inSrc = srcExisting.contains(srcCol);
        boolean inTgt = tgtExisting.contains(tgtCol);
        handleCollectionPair(
            inspector,
            sourceDb,
            targetDb,
            srcCol,
            tgtCol,
            inSrc,
            inTgt,
            validateIndexes,
            targetChunkSize,
            maxSplitsPerCollection,
            runId,
            nowIso,
            slices,
            mismatches);
      }
    } else {
      Set<String> matchedTargets = new LinkedHashSet<>();
      TreeSet<String> sortedSrc = new TreeSet<>(srcExisting);
      for (String srcCol : sortedSrc) {
        String tgtCol = forwardMap.getOrDefault(srcCol, srcCol);
        matchedTargets.add(tgtCol);
        allPairs.put(srcCol, tgtCol);
        boolean inTgt = tgtExisting.contains(tgtCol);
        handleCollectionPair(
            inspector,
            sourceDb,
            targetDb,
            srcCol,
            tgtCol,
            /* inSrc= */ true,
            inTgt,
            validateIndexes,
            targetChunkSize,
            maxSplitsPerCollection,
            runId,
            nowIso,
            slices,
            mismatches);
      }

      // Detect orphaned/extra collections present on Target Firestore that do not exist on Source.
      TreeSet<String> sortedTgt = new TreeSet<>(tgtExisting);
      for (String tgtCol : sortedTgt) {
        if (matchedTargets.contains(tgtCol)) {
          continue;
        }
        String mappedSrc = reverseMap.getOrDefault(tgtCol, tgtCol);
        allPairs.put(mappedSrc, tgtCol);
        mismatches.add(
            new MismatchedDocument(
                runId,
                sourceDb,
                targetDb,
                mappedSrc,
                tgtCol,
                "{\"$collection\":\"" + tgtCol + "\"}",
                null,
                MismatchType.EXTRA_COLLECTION_IN_TARGET,
                "Collection exists in target database '"
                    + targetDb
                    + "' but is absent in source database '"
                    + sourceDb
                    + "'",
                "catalog",
                nowIso,
                128));
      }
    }

    return new PlanResult(slices, mismatches, allPairs);
  }

  private static void handleCollectionPair(
      MetadataInspector inspector,
      String sourceDb,
      String targetDb,
      String srcCol,
      String tgtCol,
      boolean inSrc,
      boolean inTgt,
      boolean validateIndexes,
      int targetChunkSize,
      int maxSplitsPerCollection,
      String runId,
      String nowIso,
      List<ValidationSlice> slices,
      List<MismatchedDocument> mismatches) {
    if (inSrc && !inTgt) {
      mismatches.add(
          new MismatchedDocument(
              runId,
              sourceDb,
              targetDb,
              srcCol,
              tgtCol,
              "{\"$collection\":\"" + srcCol + "\"}",
              null,
              MismatchType.MISSING_COLLECTION_IN_TARGET,
              "Source collection '"
                  + srcCol
                  + "' is missing target collection '"
                  + tgtCol
                  + "' in '"
                  + targetDb
                  + "'",
              "catalog",
              nowIso,
              128));
      return;
    }
    if (!inSrc && inTgt) {
      mismatches.add(
          new MismatchedDocument(
              runId,
              sourceDb,
              targetDb,
              srcCol,
              tgtCol,
              "{\"$collection\":\"" + tgtCol + "\"}",
              null,
              MismatchType.EXTRA_COLLECTION_IN_TARGET,
              "Target collection '"
                  + tgtCol
                  + "' exists in '"
                  + targetDb
                  + "' but source collection '"
                  + srcCol
                  + "' is absent",
              "catalog",
              nowIso,
              128));
      return;
    }
    if (!inSrc) {
      return;
    }

    if (validateIndexes) {
      diffSecondaryIndexes(
          inspector, sourceDb, targetDb, srcCol, tgtCol, runId, nowIso, mismatches);
    }

    slices.addAll(
        generateCollectionSlices(
            inspector,
            srcCol,
            tgtCol,
            Math.max(1000, targetChunkSize),
            Math.max(1, maxSplitsPerCollection)));
  }

  private static void diffSecondaryIndexes(
      MetadataInspector inspector,
      String sourceDb,
      String targetDb,
      String srcCol,
      String tgtCol,
      String runId,
      String nowIso,
      List<MismatchedDocument> mismatches) {
    List<NormalizedIndexSpec> srcIndexes = inspector.listSecondaryIndexes(false, srcCol);
    List<NormalizedIndexSpec> tgtIndexes = inspector.listSecondaryIndexes(true, tgtCol);

    Map<String, NormalizedIndexSpec> srcByKey = new TreeMap<>();
    for (NormalizedIndexSpec spec : srcIndexes) {
      srcByKey.put(spec.getCanonicalKeySpec(), spec);
    }
    Map<String, NormalizedIndexSpec> tgtByKey = new TreeMap<>();
    for (NormalizedIndexSpec spec : tgtIndexes) {
      tgtByKey.put(spec.getCanonicalKeySpec(), spec);
    }

    for (Map.Entry<String, NormalizedIndexSpec> entry : srcByKey.entrySet()) {
      String keySpec = entry.getKey();
      NormalizedIndexSpec sIdx = entry.getValue();
      NormalizedIndexSpec tIdx = tgtByKey.get(keySpec);
      if (tIdx == null) {
        mismatches.add(
            new MismatchedDocument(
                runId,
                sourceDb,
                targetDb,
                srcCol,
                tgtCol,
                "{\"$index\":" + keySpec + "}",
                null,
                MismatchType.MISSING_INDEX_IN_TARGET,
                String.format(
                    "Secondary index '%s' (%s, %s) missing on target '%s'",
                    sIdx.getName(), keySpec, sIdx.describeOptions(), tgtCol),
                "index-audit",
                nowIso,
                160));
      } else if (!sIdx.optionsMatch(tIdx)) {
        mismatches.add(
            new MismatchedDocument(
                runId,
                sourceDb,
                targetDb,
                srcCol,
                tgtCol,
                "{\"$index\":" + keySpec + "}",
                null,
                MismatchType.INDEX_SPEC_MISMATCH,
                String.format(
                    "Index %s options differ: source(%s) vs target(%s)",
                    keySpec, sIdx.describeOptions(), tIdx.describeOptions()),
                "index-audit",
                nowIso,
                160));
      }
    }

    for (Map.Entry<String, NormalizedIndexSpec> entry : tgtByKey.entrySet()) {
      String keySpec = entry.getKey();
      if (!srcByKey.containsKey(keySpec)) {
        NormalizedIndexSpec tIdx = entry.getValue();
        mismatches.add(
            new MismatchedDocument(
                runId,
                sourceDb,
                targetDb,
                srcCol,
                tgtCol,
                "{\"$index\":" + keySpec + "}",
                null,
                MismatchType.EXTRA_INDEX_IN_TARGET,
                String.format(
                    "Extra secondary index '%s' (%s, %s) present on target '%s'",
                    tIdx.getName(), keySpec, tIdx.describeOptions(), tgtCol),
                "index-audit",
                nowIso,
                160));
      }
    }
  }

  private static List<ValidationSlice> generateCollectionSlices(
      MetadataInspector inspector,
      String srcCol,
      String tgtCol,
      int targetChunkSize,
      int maxSplits) {
    long totalEst =
        Math.max(
            inspector.estimatedDocumentCount(false, srcCol),
            inspector.estimatedDocumentCount(true, tgtCol));
    int desiredSplits =
        (int)
            Math.max(
                1L, Math.min((long) maxSplits, (totalEst + targetChunkSize - 1) / targetChunkSize));

    List<TypeBounds> mergedScalarBounds = new ArrayList<>();
    for (String typeName : Arrays.asList("objectId", "string", "number")) {
      TypeBounds sB = inspector.probeTypeBounds(false, srcCol, typeName);
      TypeBounds tB = inspector.probeTypeBounds(true, tgtCol, typeName);
      if (sB != null || tB != null) {
        BsonValue minId =
            minBson(sB == null ? null : sB.getMinId(), tB == null ? null : tB.getMinId());
        BsonValue maxId =
            maxBson(sB == null ? null : sB.getMaxId(), tB == null ? null : tB.getMaxId());
        long count =
            Math.max(
                sB == null ? 0L : sB.getEstimatedCount(), tB == null ? 0L : tB.getEstimatedCount());
        mergedScalarBounds.add(new TypeBounds(typeName, minId, maxId, Math.max(1L, count)));
      }
    }

    boolean hasOther =
        inspector.hasOtherTypeDocuments(false, srcCol)
            || inspector.hasOtherTypeDocuments(true, tgtCol);

    List<ValidationSlice> out = new ArrayList<>();
    if (mergedScalarBounds.isEmpty() && !hasOther) {
      // Empty collection on both sides: emit a single slice so stats still report 0/0 MATCH.
      out.add(
          new ValidationSlice(srcCol, tgtCol, srcCol + "#empty-0", SliceMode.SORTED_MERGE, "{}"));
      return out;
    }

    long scalarWeightSum = 0L;
    for (TypeBounds tb : mergedScalarBounds) {
      scalarWeightSum += tb.getEstimatedCount();
    }

    for (TypeBounds tb : mergedScalarBounds) {
      int typeSplits =
          scalarWeightSum > 0
              ? (int)
                  Math.max(
                      1L,
                      Math.round((double) desiredSplits * tb.getEstimatedCount() / scalarWeightSum))
              : 1;
      typeSplits = Math.min(maxSplits, typeSplits);
      List<BsonDocument> filters = buildGapFreeTypeFilters(tb, typeSplits);
      for (int i = 0; i < filters.size(); i++) {
        String sliceId = String.format("%s#%s-%04d", srcCol, tb.getTypeName(), i);
        out.add(
            new ValidationSlice(
                srcCol,
                tgtCol,
                sliceId,
                SliceMode.SORTED_MERGE,
                filters.get(i).toJson(EXTENDED_JSON)));
      }
    }

    if (hasOther) {
      out.add(
          new ValidationSlice(
              srcCol,
              tgtCol,
              srcCol + "#other-0000",
              SliceMode.BATCH_LOOKUP,
              OTHER_TYPES_FILTER_JSON));
    }

    return out;
  }

  /**
   * Builds disjoint, gap-free {@code (-inf, b1), [b1, b2), ..., [b_{k-1}, +inf)} filters within a
   * single BSON {@code $type} so any document of that type is guaranteed to land in exactly one
   * slice even if inserted outside probed min/max bounds during live replication.
   */
  private static List<BsonDocument> buildGapFreeTypeFilters(TypeBounds bounds, int numSplits) {
    BsonValue typeSelector =
        "number".equals(bounds.getTypeName()) ? NUMBER_TYPES : new BsonString(bounds.getTypeName());

    if (numSplits <= 1
        || BsonOrderComparator.INSTANCE.compare(bounds.getMinId(), bounds.getMaxId()) >= 0) {
      return Collections.singletonList(
          new BsonDocument("_id", new BsonDocument("$type", typeSelector)));
    }

    List<BsonValue> splitPoints = interpolateSplitPoints(bounds, numSplits);
    if (splitPoints.isEmpty()) {
      return Collections.singletonList(
          new BsonDocument("_id", new BsonDocument("$type", typeSelector)));
    }

    List<BsonDocument> filters = new ArrayList<>(splitPoints.size() + 1);
    for (int i = 0; i <= splitPoints.size(); i++) {
      BsonDocument idPred = new BsonDocument("$type", typeSelector);
      if (i > 0) {
        idPred.append("$gte", splitPoints.get(i - 1));
      }
      if (i < splitPoints.size()) {
        idPred.append("$lt", splitPoints.get(i));
      }
      filters.add(new BsonDocument("_id", idPred));
    }
    return filters;
  }

  private static List<BsonValue> interpolateSplitPoints(TypeBounds bounds, int numSplits) {
    List<BsonValue> points = new ArrayList<>();
    String typeName = bounds.getTypeName();

    if ("objectId".equals(typeName)
        && bounds.getMinId().isObjectId()
        && bounds.getMaxId().isObjectId()) {
      BigInteger minInt =
          new BigInteger(1, bounds.getMinId().asObjectId().getValue().toByteArray());
      BigInteger maxInt =
          new BigInteger(1, bounds.getMaxId().asObjectId().getValue().toByteArray());
      BigInteger span = maxInt.subtract(minInt);
      if (span.compareTo(BigInteger.valueOf(numSplits)) < 0) {
        return points;
      }
      BsonValue prev = null;
      for (int i = 1; i < numSplits; i++) {
        BigInteger cut =
            minInt.add(span.multiply(BigInteger.valueOf(i)).divide(BigInteger.valueOf(numSplits)));
        BsonObjectId candidate = new BsonObjectId(new ObjectId(toTwelveBytes(cut)));
        if (prev == null || BsonOrderComparator.INSTANCE.compare(prev, candidate) < 0) {
          points.add(candidate);
          prev = candidate;
        }
      }
      return points;
    }

    if ("number".equals(typeName)) {
      double minD = toDoubleApprox(bounds.getMinId());
      double maxD = toDoubleApprox(bounds.getMaxId());
      if (!Double.isFinite(minD) || !Double.isFinite(maxD) || maxD <= minD) {
        return points;
      }
      double step = (maxD - minD) / numSplits;
      boolean integral =
          bounds.getMinId().isInt32()
              || bounds.getMinId().isInt64()
              || bounds.getMaxId().isInt32()
              || bounds.getMaxId().isInt64();
      BsonValue prev = null;
      for (int i = 1; i < numSplits; i++) {
        double rawCut = minD + step * i;
        BsonValue candidate = integral ? new BsonInt64(Math.round(rawCut)) : new BsonDouble(rawCut);
        if (prev == null || BsonOrderComparator.INSTANCE.compare(prev, candidate) < 0) {
          points.add(candidate);
          prev = candidate;
        }
      }
      return points;
    }

    if ("string".equals(typeName) && bounds.getMinId().isString() && bounds.getMaxId().isString()) {
      BigInteger minInt = stringPrefixToUint64(bounds.getMinId().asString().getValue());
      BigInteger maxInt = stringPrefixToUint64(bounds.getMaxId().asString().getValue());
      BigInteger span = maxInt.subtract(minInt);
      if (span.compareTo(BigInteger.ZERO) <= 0) {
        return points;
      }
      BsonValue prev = null;
      for (int i = 1; i < numSplits; i++) {
        BigInteger cut =
            minInt.add(span.multiply(BigInteger.valueOf(i)).divide(BigInteger.valueOf(numSplits)));
        String cutStr = uint64ToAsciiPrefix(cut);
        if (!cutStr.isEmpty()) {
          BsonString candidate = new BsonString(cutStr);
          if (prev == null || BsonOrderComparator.INSTANCE.compare(prev, candidate) < 0) {
            points.add(candidate);
            prev = candidate;
          }
        }
      }
    }
    return points;
  }

  private static byte[] toTwelveBytes(BigInteger val) {
    byte[] raw = val.toByteArray();
    byte[] twelve = new byte[12];
    int copyLen = Math.min(raw.length, 12);
    System.arraycopy(raw, raw.length - copyLen, twelve, 12 - copyLen, copyLen);
    return twelve;
  }

  private static double toDoubleApprox(BsonValue v) {
    if (v.isInt32()) {
      return v.asInt32().getValue();
    }
    if (v.isInt64()) {
      return (double) v.asInt64().getValue();
    }
    if (v.isDouble()) {
      return v.asDouble().getValue();
    }
    if (v.isDecimal128()) {
      return v.asDecimal128().decimal128Value().bigDecimalValue().doubleValue();
    }
    return 0.0d;
  }

  private static BigInteger stringPrefixToUint64(String s) {
    byte[] utf8 = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    byte[] eight = new byte[8];
    for (int i = 0; i < 8 && i < utf8.length; i++) {
      // Clamp to printable 7-bit ASCII [0x20..0x7E] so generated split strings remain valid UTF-8.
      int b = Byte.toUnsignedInt(utf8[i]);
      eight[i] = (byte) Math.max(0x20, Math.min(0x7E, b));
    }
    return new BigInteger(1, eight);
  }

  private static String uint64ToAsciiPrefix(BigInteger val) {
    byte[] raw = val.toByteArray();
    byte[] eight = new byte[8];
    int copyLen = Math.min(raw.length, 8);
    System.arraycopy(raw, raw.length - copyLen, eight, 8 - copyLen, copyLen);
    StringBuilder sb = new StringBuilder(8);
    for (byte b : eight) {
      int u = Byte.toUnsignedInt(b);
      if (u >= 0x20 && u <= 0x7E) {
        sb.append((char) u);
      } else if (u > 0x7E) {
        sb.append('~');
      }
    }
    return sb.toString();
  }

  private static BsonValue minBson(@Nullable BsonValue a, @Nullable BsonValue b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return BsonOrderComparator.INSTANCE.compare(a, b) <= 0 ? a : b;
  }

  private static BsonValue maxBson(@Nullable BsonValue a, @Nullable BsonValue b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return BsonOrderComparator.INSTANCE.compare(a, b) >= 0 ? a : b;
  }

  public static Map<String, String> parseCollectionMappings(String mappingsCsv) {
    Map<String, String> out = new LinkedHashMap<>();
    if (mappingsCsv == null || mappingsCsv.isBlank()) {
      return out;
    }
    for (String pair : mappingsCsv.split(",")) {
      String trimmed = pair.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      int idx = trimmed.indexOf(':');
      if (idx <= 0 || idx == trimmed.length() - 1) {
        throw new IllegalArgumentException(
            "Invalid collectionMappings entry '"
                + trimmed
                + "'; expected format 'sourceCol:targetCol'");
      }
      out.put(trimmed.substring(0, idx).trim(), trimmed.substring(idx + 1).trim());
    }
    return out;
  }

  private static Set<String> parseCsvSet(String csv) {
    Set<String> out = new LinkedHashSet<>();
    if (csv == null || csv.isBlank()) {
      return out;
    }
    for (String item : csv.split(",")) {
      String trimmed = item.trim();
      if (!trimmed.isEmpty()) {
        out.add(trimmed);
      }
    }
    return out;
  }

  /** Production {@link MetadataInspector} backed by MongoDB Java driver clients. */
  private static final class LiveMongoMetadataInspector implements MetadataInspector {
    private final MongoDatabase srcDb;
    private final MongoDatabase tgtDb;

    LiveMongoMetadataInspector(MongoDatabase srcDb, MongoDatabase tgtDb) {
      this.srcDb = srcDb;
      this.tgtDb = tgtDb;
    }

    private MongoDatabase db(boolean isTarget) {
      return isTarget ? tgtDb : srcDb;
    }

    @Override
    public Set<String> listUserCollections(boolean isTarget) {
      Set<String> cols = new TreeSet<>();
      for (String name : db(isTarget).listCollectionNames()) {
        if (name != null && !name.startsWith("system.")) {
          cols.add(name);
        }
      }
      return cols;
    }

    @Override
    public List<NormalizedIndexSpec> listSecondaryIndexes(boolean isTarget, String collection) {
      List<NormalizedIndexSpec> specs = new ArrayList<>();
      try {
        MongoCollection<BsonDocument> col =
            db(isTarget).getCollection(collection, BsonDocument.class);
        for (BsonDocument idx : col.listIndexes(BsonDocument.class)) {
          String name = idx.getString("name", new BsonString("")).getValue();
          if ("_id_".equals(name)) {
            continue;
          }
          BsonDocument keyDoc = idx.getDocument("key", new BsonDocument());
          if (keyDoc.size() == 1 && keyDoc.containsKey("_id")) {
            continue;
          }
          boolean unique = idx.getBoolean("unique", org.bson.BsonBoolean.FALSE).getValue();
          boolean sparse = idx.getBoolean("sparse", org.bson.BsonBoolean.FALSE).getValue();
          Long ttl = null;
          if (idx.containsKey("expireAfterSeconds") && idx.get("expireAfterSeconds").isNumber()) {
            ttl = idx.getNumber("expireAfterSeconds").longValue();
          }
          specs.add(
              new NormalizedIndexSpec(name, normalizeIndexKeySpec(keyDoc), unique, sparse, ttl));
        }
      } catch (Exception e) {
        LOG.warn(
            "Unable to list secondary indexes on {} collection '{}': {}",
            isTarget ? "target" : "source",
            collection,
            e.getMessage());
      }
      return specs;
    }

    private static String normalizeIndexKeySpec(BsonDocument keyDoc) {
      BsonDocument normalized = new BsonDocument();
      for (Map.Entry<String, BsonValue> e : keyDoc.entrySet()) {
        BsonValue v = e.getValue();
        if (v.isNumber()) {
          normalized.put(e.getKey(), new BsonInt32(v.asNumber().intValue() < 0 ? -1 : 1));
        } else {
          normalized.put(e.getKey(), v);
        }
      }
      return normalized.toJson();
    }

    @Override
    public long estimatedDocumentCount(boolean isTarget, String collection) {
      try {
        return db(isTarget).getCollection(collection).estimatedDocumentCount();
      } catch (Exception e) {
        return 0L;
      }
    }

    @Override
    @Nullable
    public TypeBounds probeTypeBounds(boolean isTarget, String collection, String typeName) {
      try {
        MongoCollection<BsonDocument> col =
            db(isTarget).getCollection(collection, BsonDocument.class);
        BsonValue typeVal = "number".equals(typeName) ? NUMBER_TYPES : new BsonString(typeName);
        BsonDocument filter = new BsonDocument("_id", new BsonDocument("$type", typeVal));
        BsonDocument proj = new BsonDocument("_id", new BsonInt32(1));
        BsonDocument minDoc =
            col.find(filter)
                .projection(proj)
                .sort(new BsonDocument("_id", new BsonInt32(1)))
                .limit(1)
                .first();
        if (minDoc == null || !minDoc.containsKey("_id")) {
          return null;
        }
        BsonDocument maxDoc =
            col.find(filter)
                .projection(proj)
                .sort(new BsonDocument("_id", new BsonInt32(-1)))
                .limit(1)
                .first();
        BsonValue minId = minDoc.get("_id");
        BsonValue maxId = (maxDoc != null && maxDoc.containsKey("_id")) ? maxDoc.get("_id") : minId;
        long est = Math.max(1L, estimatedDocumentCount(isTarget, collection));
        return new TypeBounds(typeName, minId, maxId, est);
      } catch (Exception e) {
        LOG.warn(
            "Type bound probe failed on {} '{}.{}': {}",
            isTarget ? "target" : "source",
            collection,
            typeName,
            e.getMessage());
        return null;
      }
    }

    @Override
    public boolean hasOtherTypeDocuments(boolean isTarget, String collection) {
      try {
        MongoCollection<BsonDocument> col =
            db(isTarget).getCollection(collection, BsonDocument.class);
        BsonDocument doc =
            col.find(BsonDocument.parse(OTHER_TYPES_FILTER_JSON))
                .projection(new BsonDocument("_id", new BsonInt32(1)))
                .limit(1)
                .first();
        return doc != null;
      } catch (Exception e) {
        return false;
      }
    }
  }
}
