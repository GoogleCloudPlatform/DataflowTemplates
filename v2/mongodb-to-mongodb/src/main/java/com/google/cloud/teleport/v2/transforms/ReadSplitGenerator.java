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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
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
public class ReadSplitGenerator {

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
   * Generates a list of BsonDocument filter queries using data-driven quantile sampling or
   * automatic key-type discovery.
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

    Set<IdType> activeTypes =
        client != null
            ? detectIdTypes(client, databaseName, collectionName)
            : EnumSet.allOf(IdType.class);

    if (client == null) {
      return generateIndexSliceFilters(numSplits, activeTypes);
    }

    MongoCollection<BsonDocument> col =
        client.getDatabase(databaseName).getCollection(collectionName, BsonDocument.class);

    List<BsonDocument> numberFilters = Collections.emptyList();
    if (activeTypes.contains(IdType.NUMBER)) {
      try {
        numberFilters =
            discoverSplitsForType(
                col, numSplits, new BsonDocument("_id", new BsonDocument("$type", NUMBER_BSON_TYPES)));
      } catch (Exception e) {
        LOG.warn(
            "Data-driven splits failed for NUMBER type in '{}.{}' ({}). Falling back to uniform splits.",
            databaseName,
            collectionName,
            e.getMessage());
        numberFilters = generateNumberFilters(numSplits);
      }
    }

    List<BsonDocument> stringFilters = Collections.emptyList();
    if (activeTypes.contains(IdType.STRING)) {
      try {
        stringFilters =
            discoverSplitsForType(
                col, numSplits, new BsonDocument("_id", new BsonDocument("$type", new BsonString("string"))));
      } catch (Exception e) {
        LOG.warn(
            "Data-driven splits failed for STRING type in '{}.{}' ({}). Falling back to uniform splits.",
            databaseName,
            collectionName,
            e.getMessage());
        stringFilters = generateStringFilters(numSplits);
      }
    }

    List<BsonDocument> objectIdFilters = Collections.emptyList();
    if (activeTypes.contains(IdType.OBJECT_ID)) {
      try {
        objectIdFilters =
            discoverSplitsForType(
                col, numSplits, new BsonDocument("_id", new BsonDocument("$type", new BsonString("objectId"))));
      } catch (Exception e) {
        LOG.warn(
            "Data-driven splits failed for OBJECT_ID type in '{}.{}' ({}). Falling back to uniform splits.",
            databaseName,
            collectionName,
            e.getMessage());
        objectIdFilters = generateObjectIdFilters(numSplits);
      }
    }

    List<BsonDocument> otherFilters = Collections.emptyList();
    if (activeTypes.contains(IdType.OTHER)) {
      try {
        otherFilters =
            discoverSplitsForType(
                col,
                numSplits,
                new BsonDocument("_id", new BsonDocument("$not", new BsonDocument("$type", KNOWN_BSON_TYPES))));
      } catch (Exception e) {
        LOG.warn(
            "Data-driven splits failed for OTHER type in '{}.{}' ({}).",
            databaseName,
            collectionName,
            e.getMessage());
      }
    }

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
      if (!otherFilters.isEmpty() && i < otherFilters.size()) {
        branchFilters.add(otherFilters.get(i));
      } else if (i == 0 && activeTypes.contains(IdType.OTHER)) {
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

  private static final BsonArray KNOWN_BSON_TYPES =
      new BsonArray(
          Arrays.asList(
              new BsonString("string"),
              new BsonString("objectId"),
              new BsonString("int"),
              new BsonString("long"),
              new BsonString("double"),
              new BsonString("decimal")));

  /**
   * Detects which _id BSON types are present in a MongoDB collection using lightweight limit(1)
   * probes.
   */
  public static Set<IdType> detectIdTypes(
      MongoClient client, String databaseName, String collectionName) {
    EnumSet<IdType> activeTypes = EnumSet.noneOf(IdType.class);
    MongoDatabase db = client.getDatabase(databaseName);
    MongoCollection<BsonDocument> col = db.getCollection(collectionName, BsonDocument.class);

    if (col.find(new BsonDocument("_id", new BsonDocument("$type", new BsonString("string"))))
            .limit(1)
            .first()
        != null) {
      activeTypes.add(IdType.STRING);
    }
    if (col.find(new BsonDocument("_id", new BsonDocument("$type", new BsonString("objectId"))))
            .limit(1)
            .first()
        != null) {
      activeTypes.add(IdType.OBJECT_ID);
    }
    if (col.find(new BsonDocument("_id", new BsonDocument("$type", NUMBER_BSON_TYPES)))
            .limit(1)
            .first()
        != null) {
      activeTypes.add(IdType.NUMBER);
    }
    if (col.find(
                new BsonDocument(
                    "_id", new BsonDocument("$not", new BsonDocument("$type", KNOWN_BSON_TYPES))))
            .limit(1)
            .first()
        != null) {
      activeTypes.add(IdType.OTHER);
    }

    if (activeTypes.isEmpty()) {
      activeTypes.addAll(EnumSet.allOf(IdType.class));
    }
    return activeTypes;
  }

  private static List<BsonDocument> discoverSplitsForType(
      MongoCollection<BsonDocument> col, int numSplits, BsonDocument typeMatch) {
    if (numSplits <= 1) {
      return Collections.singletonList(typeMatch);
    }
    
    int sampleSize = Math.max(1000, numSplits * 64);
    List<BsonDocument> pipeline =
        Arrays.asList(
            new BsonDocument("$match", typeMatch),
            new BsonDocument("$sample", new BsonDocument("size", new BsonInt32(sampleSize))),
            new BsonDocument("$project", new BsonDocument("_id", new BsonInt32(1))),
            new BsonDocument("$sort", new BsonDocument("_id", new BsonInt32(1))));

    List<BsonValue> sampledKeys = new ArrayList<>();
    for (BsonDocument doc : col.aggregate(pipeline)) {
      if (doc.containsKey("_id")) {
        sampledKeys.add(doc.get("_id"));
      }
    }

    if (sampledKeys.size() < numSplits) {
      throw new IllegalArgumentException(
          "Insufficient sample size: sampled "
              + sampledKeys.size()
              + " keys, required at least "
              + numSplits);
    }

    List<BsonValue> boundaries = new ArrayList<>();
    int step = sampledKeys.size() / numSplits;
    for (int i = 1; i < numSplits; i++) {
      BsonValue boundary = sampledKeys.get(i * step);
      if (!boundaries.isEmpty() && boundary.equals(boundaries.get(boundaries.size() - 1))) {
        throw new IllegalArgumentException("Sampled quantile boundaries contain duplicates");
      }
      boundaries.add(boundary);
    }

    List<BsonDocument> slices = new ArrayList<>();
    for (int i = 0; i < numSplits; i++) {
      BsonDocument idDoc = new BsonDocument();
      BsonDocument typeMatchId = typeMatch.getDocument("_id");
      for (String key : typeMatchId.keySet()) {
        idDoc.append(key, typeMatchId.get(key));
      }

      if (i == 0) {
        idDoc.append("$lt", boundaries.get(0));
      } else if (i == numSplits - 1) {
        idDoc.append("$gte", boundaries.get(boundaries.size() - 1));
      } else {
        idDoc.append("$gte", boundaries.get(i - 1)).append("$lt", boundaries.get(i));
      }
      slices.add(new BsonDocument("_id", idDoc));
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
                  "{\"_id\": {\"$gte\": {\"$oid\": \"%s\"}, \"%s\": {\"$oid\": \"%s\"}}}",
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
    long minHex = 0x00000000L;
    long maxHex = 0xffffffffL;
    long step = (maxHex - minHex) / numSplits;
    for (int i = 0; i <= numSplits; i++) {
      if (i == 0) {
        bounds.add("000000000000000000000000");
      } else if (i == numSplits) {
        bounds.add("ffffffffffffffffffffffff");
      } else {
        long val = minHex + i * step;
        String hexPrefix = String.format("%08x", val);
        bounds.add(hexPrefix + "0000000000000000");
      }
    }
    return bounds;
  }
}
