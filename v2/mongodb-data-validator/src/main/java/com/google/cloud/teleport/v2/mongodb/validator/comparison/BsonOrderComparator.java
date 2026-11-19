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
package com.google.cloud.teleport.v2.mongodb.validator.comparison;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonNumber;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.types.Decimal128;

/**
 * Comparator implementing the canonical BSON sort order shared by MongoDB and Firestore with
 * MongoDB compatibility, so lockstep sorted merge-joins advance source and target cursors
 * identically.
 */
public final class BsonOrderComparator implements Comparator<BsonValue>, Serializable {

  private static final long serialVersionUID = 1L;

  public static final BsonOrderComparator INSTANCE = new BsonOrderComparator();

  @Override
  public int compare(BsonValue a, BsonValue b) {
    if (a == b) {
      return 0;
    }
    if (a == null) {
      return -1;
    }
    if (b == null) {
      return 1;
    }

    int rankA = typeRank(a.getBsonType());
    int rankB = typeRank(b.getBsonType());
    if (rankA != rankB) {
      return Integer.compare(rankA, rankB);
    }

    switch (a.getBsonType()) {
      case MIN_KEY:
      case MAX_KEY:
      case NULL:
      case UNDEFINED:
        return 0;

      case INT32:
      case INT64:
      case DOUBLE:
      case DECIMAL128:
        return compareNumbers(a, b);

      case STRING:
      case SYMBOL:
        return compareUtf8Strings(extractString(a), extractString(b));

      case OBJECT_ID:
        return Arrays.compareUnsigned(
            a.asObjectId().getValue().toByteArray(), b.asObjectId().getValue().toByteArray());

      case BOOLEAN:
        return Boolean.compare(a.asBoolean().getValue(), b.asBoolean().getValue());

      case DATE_TIME:
        return Long.compare(a.asDateTime().getValue(), b.asDateTime().getValue());

      case TIMESTAMP:
        BsonTimestamp tsA = a.asTimestamp();
        BsonTimestamp tsB = b.asTimestamp();
        int secCmp = Integer.compareUnsigned(tsA.getTime(), tsB.getTime());
        return secCmp != 0 ? secCmp : Integer.compareUnsigned(tsA.getInc(), tsB.getInc());

      case BINARY:
        BsonBinary binA = a.asBinary();
        BsonBinary binB = b.asBinary();
        int lenCmp = Integer.compare(binA.getData().length, binB.getData().length);
        if (lenCmp != 0) {
          return lenCmp;
        }
        int subCmp =
            Integer.compare(Byte.toUnsignedInt(binA.getType()), Byte.toUnsignedInt(binB.getType()));
        return subCmp != 0 ? subCmp : Arrays.compareUnsigned(binA.getData(), binB.getData());

      case REGULAR_EXPRESSION:
        BsonRegularExpression reA = a.asRegularExpression();
        BsonRegularExpression reB = b.asRegularExpression();
        int patCmp = compareUtf8Strings(reA.getPattern(), reB.getPattern());
        return patCmp != 0 ? patCmp : compareUtf8Strings(reA.getOptions(), reB.getOptions());

      case DOCUMENT:
        return compareDocuments(a.asDocument(), b.asDocument());

      case ARRAY:
        return compareArrays(a.asArray(), b.asArray());

      default:
        return compareUtf8Strings(a.toString(), b.toString());
    }
  }

  /**
   * MongoDB BSON type comparison rank order: MinKey(1) &lt; Null/Undefined(2) &lt; Numbers(3) &lt;
   * String/Symbol(4) &lt; Object(5) &lt; Array(6) &lt; BinData(7) &lt; ObjectId(8) &lt; Boolean(9)
   * &lt; Date(10) &lt; Timestamp(11) &lt; Regex(12) &lt; MaxKey(99).
   */
  public static int typeRank(BsonType type) {
    switch (type) {
      case MIN_KEY:
        return 1;
      case NULL:
      case UNDEFINED:
        return 2;
      case INT32:
      case INT64:
      case DOUBLE:
      case DECIMAL128:
        return 3;
      case STRING:
      case SYMBOL:
        return 4;
      case DOCUMENT:
        return 5;
      case ARRAY:
        return 6;
      case BINARY:
        return 7;
      case OBJECT_ID:
        return 8;
      case BOOLEAN:
        return 9;
      case DATE_TIME:
        return 10;
      case TIMESTAMP:
        return 11;
      case REGULAR_EXPRESSION:
        return 12;
      case MAX_KEY:
        return 99;
      default:
        return 50;
    }
  }

  private static int compareUtf8Strings(String s1, String s2) {
    return Arrays.compareUnsigned(
        s1.getBytes(StandardCharsets.UTF_8), s2.getBytes(StandardCharsets.UTF_8));
  }

  private static String extractString(BsonValue v) {
    return v.getBsonType() == BsonType.SYMBOL ? v.asSymbol().getSymbol() : v.asString().getValue();
  }

  private static int compareNumbers(BsonValue a, BsonValue b) {
    boolean nanA = isNaN(a);
    boolean nanB = isNaN(b);
    if (nanA && nanB) {
      return 0;
    }
    // In MongoDB BSON ordering, NaN sorts before all other numeric values (including -Infinity).
    if (nanA) {
      return -1;
    }
    if (nanB) {
      return 1;
    }

    int infA = infinitySign(a);
    int infB = infinitySign(b);
    if (infA != 0 || infB != 0) {
      if (infA == infB) {
        return 0;
      }
      if (infA < 0 || infB > 0) {
        return -1;
      }
      return 1;
    }

    return toBigDecimal(a).compareTo(toBigDecimal(b));
  }

  private static boolean isNaN(BsonValue v) {
    if (v.getBsonType() == BsonType.DOUBLE) {
      return Double.isNaN(v.asDouble().getValue());
    }
    if (v.getBsonType() == BsonType.DECIMAL128) {
      return v.asDecimal128().getValue().isNaN();
    }
    return false;
  }

  private static int infinitySign(BsonValue v) {
    if (v.getBsonType() == BsonType.DOUBLE) {
      double d = v.asDouble().getValue();
      if (Double.isInfinite(d)) {
        return d < 0 ? -1 : 1;
      }
    } else if (v.getBsonType() == BsonType.DECIMAL128) {
      Decimal128 dec = v.asDecimal128().getValue();
      if (dec.isInfinite()) {
        return dec.isNegative() ? -1 : 1;
      }
    }
    return 0;
  }

  private static BigDecimal toBigDecimal(BsonValue v) {
    switch (v.getBsonType()) {
      case INT32:
        return BigDecimal.valueOf(v.asInt32().getValue());
      case INT64:
        return BigDecimal.valueOf(v.asInt64().getValue());
      case DOUBLE:
        double d = v.asDouble().getValue();
        return d == 0.0d ? BigDecimal.ZERO : new BigDecimal(d);
      case DECIMAL128:
        Decimal128 dec = v.asDecimal128().getValue();
        return dec.bigDecimalValue();
      default:
        return BigDecimal.valueOf(((BsonNumber) v).longValue());
    }
  }

  private int compareDocuments(BsonDocument a, BsonDocument b) {
    Iterator<Map.Entry<String, BsonValue>> itA = a.entrySet().iterator();
    Iterator<Map.Entry<String, BsonValue>> itB = b.entrySet().iterator();
    while (itA.hasNext() && itB.hasNext()) {
      Map.Entry<String, BsonValue> eA = itA.next();
      Map.Entry<String, BsonValue> eB = itB.next();
      int typeCmp =
          Integer.compare(
              typeRank(eA.getValue().getBsonType()), typeRank(eB.getValue().getBsonType()));
      if (typeCmp != 0) {
        return typeCmp;
      }
      int keyCmp = compareUtf8Strings(eA.getKey(), eB.getKey());
      if (keyCmp != 0) {
        return keyCmp;
      }
      int valCmp = compare(eA.getValue(), eB.getValue());
      if (valCmp != 0) {
        return valCmp;
      }
    }
    return Boolean.compare(itA.hasNext(), itB.hasNext());
  }

  private int compareArrays(BsonArray a, BsonArray b) {
    int minLen = Math.min(a.size(), b.size());
    for (int i = 0; i < minLen; i++) {
      int cmp = compare(a.get(i), b.get(i));
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(a.size(), b.size());
  }
}
