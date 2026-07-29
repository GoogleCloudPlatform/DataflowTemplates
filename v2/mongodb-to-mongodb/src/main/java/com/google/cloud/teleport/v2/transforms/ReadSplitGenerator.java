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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.bson.BsonDocument;

/**
 * Utility class to generate orthogonal BSON filter queries for parallel index-slice reading
 * without requiring MongoDB splitVector or bucketAuto commands.
 */
public class ReadSplitGenerator {

  private ReadSplitGenerator() {}

  /**
   * Generates a list of BsonDocument filter queries that partition a MongoDB collection across all
   * BSON data types (Numbers, Strings, ObjectIds, BinData, and remaining types) with zero
   * duplicates and 100% coverage.
   *
   * @param numSplits Total target number of parallel read splits.
   * @return List of BsonDocument filters.
   */
  public static List<BsonDocument> generateIndexSliceFilters(int numSplits) {
    if (numSplits <= 1) {
      return Collections.singletonList(new BsonDocument());
    }

    List<BsonDocument> filters = new ArrayList<>();

    int numNumberSplits = Math.max(1, numSplits / 4);
    int numStringSplits = Math.max(1, numSplits / 4);
    int numObjectIdSplits = Math.max(1, numSplits / 2);

    // 1. Integer / Long modulo slices: {"_id": {"$type": ["int", "long"], "$mod": [M, r]}}
    for (int r = 0; r < numNumberSplits; r++) {
      BsonDocument filter =
          BsonDocument.parse(
              String.format(
                  "{\"_id\": {\"$type\": [\"int\", \"long\"], \"$mod\": [%d, %d]}}",
                  numNumberSplits, r));
      filters.add(filter);
    }

    // 2. Double / Decimal slice: {"_id": {"$type": ["double", "decimal"]}}
    filters.add(BsonDocument.parse("{\"_id\": {\"$type\": [\"double\", \"decimal\"]}}"));

    // 3. String ASCII prefix slices: {"_id": {"$type": "string", "$gte": "...", "$lt": "..."}}
    List<String> stringBounds = generateStringBounds(numStringSplits);
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

    // 4. ObjectId hex timestamp slices: {"_id": {"$gte": ObjectId("..."), "$lt": ObjectId("...")}}
    List<String> hexBounds = generateObjectIdBounds(numObjectIdSplits);
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

    // 5. BinData / UUID slice: {"_id": {"$type": "binData"}}
    filters.add(BsonDocument.parse("{\"_id\": {\"$type\": \"binData\"}}"));

    // 6. Catch-All slice for remaining BSON types (bool, date, object, array, null, etc.)
    filters.add(
        BsonDocument.parse(
            "{\"_id\": {\"$not\": {\"$type\": [\"int\", \"long\", \"double\", \"decimal\","
                + " \"string\", \"objectId\", \"binData\"]}}}"));

    return filters;
  }

  private static List<String> generateStringBounds(int numSplits) {
    List<String> bounds = new ArrayList<>();
    bounds.add("");
    if (numSplits == 1) {
      bounds.add("\uffff");
      return bounds;
    }
    int startChar = 48; // '0'
    int endChar = 122; // 'z'
    int step = Math.max(1, (endChar - startChar) / numSplits);
    for (int i = 1; i < numSplits; i++) {
      int c = Math.min(endChar, startChar + i * step);
      bounds.add(String.valueOf((char) c));
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
