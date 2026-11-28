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

import com.google.cloud.teleport.v2.mongodb.validator.util.BsonEncodingUtils;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.types.Decimal128;

/**
 * Deep canonical BSON document comparator that preserves exact BSON types, {@code Binary} subtypes,
 * {@code NaN} semantics, {@code int64 > 2^53} integer precision, embedded subdocument key ordering,
 * and raw wire duplicate-key detection.
 */
public final class BsonDocumentComparator implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final long MAX_EXACT_FLOAT_INT = 1L << 53;
  private static final double NUMERIC_TOLERANCE = 1e-9d;

  /** Configuration options controlling comparison strictness and excluded paths. */
  public static final class Config implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean strictTypeChecking;
    private final boolean ignoreSubdocumentFieldOrder;
    private final ImmutableSet<String> ignoredFieldPaths;
    private final int maxDiffsPerDocument;

    public Config(
        boolean strictTypeChecking,
        boolean ignoreSubdocumentFieldOrder,
        Set<String> ignoredFieldPaths,
        int maxDiffsPerDocument) {
      this.strictTypeChecking = strictTypeChecking;
      this.ignoreSubdocumentFieldOrder = ignoreSubdocumentFieldOrder;
      this.ignoredFieldPaths =
          ignoredFieldPaths == null ? ImmutableSet.of() : ImmutableSet.copyOf(ignoredFieldPaths);
      this.maxDiffsPerDocument = Math.max(1, maxDiffsPerDocument);
    }

    public static Config strictDefault() {
      return new Config(true, false, Collections.emptySet(), 10);
    }

    public static Set<String> parseIgnoredFields(String csv) {
      if (csv == null || csv.isBlank()) {
        return ImmutableSet.of();
      }
      ImmutableSet.Builder<String> builder = ImmutableSet.builder();
      for (String part : csv.split(",")) {
        String trimmed = part.trim();
        if (!trimmed.isEmpty()) {
          builder.add(trimmed);
        }
      }
      return builder.build();
    }

    public boolean isStrictTypeChecking() {
      return strictTypeChecking;
    }

    public boolean isIgnoreSubdocumentFieldOrder() {
      return ignoreSubdocumentFieldOrder;
    }

    public Set<String> getIgnoredFieldPaths() {
      return ignoredFieldPaths;
    }

    public int getMaxDiffsPerDocument() {
      return maxDiffsPerDocument;
    }
  }

  private final Config config;

  public BsonDocumentComparator(Config config) {
    this.config = Objects.requireNonNull(config, "config");
  }

  /**
   * Compares a source and target document, returning an empty list when equal or up to {@code
   * maxDiffsPerDocument} human-readable field discrepancy descriptions.
   */
  public List<String> compare(BsonDocument sourceDoc, BsonDocument targetDoc) {
    List<String> diffs = new ArrayList<>();

    if (sourceDoc instanceof RawBsonDocument) {
      for (String dup : BsonEncodingUtils.findDuplicateKeys((RawBsonDocument) sourceDoc)) {
        if (!isIgnored(dup)) {
          addDiff(diffs, String.format("duplicate BSON key '%s' in source document", dup));
        }
      }
    }
    if (targetDoc instanceof RawBsonDocument) {
      for (String dup : BsonEncodingUtils.findDuplicateKeys((RawBsonDocument) targetDoc)) {
        if (!isIgnored(dup)) {
          addDiff(diffs, String.format("duplicate BSON key '%s' in target document", dup));
        }
      }
    }

    BsonDocument srcMat = BsonEncodingUtils.materialize(sourceDoc);
    BsonDocument tgtMat = BsonEncodingUtils.materialize(targetDoc);
    compareDocumentNodes("", srcMat, tgtMat, /* isRoot= */ true, diffs);
    return diffs;
  }

  private void compareDocumentNodes(
      String prefix, BsonDocument src, BsonDocument tgt, boolean isRoot, List<String> diffs) {
    if (diffs.size() >= config.getMaxDiffsPerDocument()) {
      return;
    }

    List<String> srcKeys = new ArrayList<>();
    for (String k : src.keySet()) {
      String path = prefix.isEmpty() ? k : prefix + "." + k;
      if (!isIgnored(path)) {
        srcKeys.add(k);
      }
    }

    List<String> tgtKeys = new ArrayList<>();
    for (String k : tgt.keySet()) {
      String path = prefix.isEmpty() ? k : prefix + "." + k;
      if (!isIgnored(path)) {
        tgtKeys.add(k);
      }
    }

    for (String k : srcKeys) {
      String path = prefix.isEmpty() ? k : prefix + "." + k;
      if (!tgt.containsKey(k)) {
        addDiff(diffs, String.format("field '%s' missing in target", path));
        if (diffs.size() >= config.getMaxDiffsPerDocument()) {
          return;
        }
      }
    }

    for (String k : tgtKeys) {
      String path = prefix.isEmpty() ? k : prefix + "." + k;
      if (!src.containsKey(k)) {
        addDiff(diffs, String.format("unexpected field '%s' in target", path));
        if (diffs.size() >= config.getMaxDiffsPerDocument()) {
          return;
        }
      }
    }

    // In MongoDB MQL, embedded subdocument equality depends on key order, whereas top-level
    // document field order can vary across drivers unless subdocument ordering is violated.
    if (!isRoot
        && !config.isIgnoreSubdocumentFieldOrder()
        && srcKeys.size() == tgtKeys.size()
        && !srcKeys.equals(tgtKeys)
        && tgt.keySet().containsAll(srcKeys)) {
      addDiff(
          diffs,
          String.format(
              "subdocument '%s' key order mismatch (src=%s, tgt=%s)", prefix, srcKeys, tgtKeys));
      if (diffs.size() >= config.getMaxDiffsPerDocument()) {
        return;
      }
    }

    for (String k : srcKeys) {
      if (!tgt.containsKey(k)) {
        continue;
      }
      String path = prefix.isEmpty() ? k : prefix + "." + k;
      compareValues(path, src.get(k), tgt.get(k), diffs);
      if (diffs.size() >= config.getMaxDiffsPerDocument()) {
        return;
      }
    }
  }

  private void compareValues(String path, BsonValue s, BsonValue t, List<String> diffs) {
    if (diffs.size() >= config.getMaxDiffsPerDocument()) {
      return;
    }
    if (s == null && t == null) {
      return;
    }
    if (s == null || t == null) {
      addDiff(diffs, String.format("%s: null reference mismatch (src=%s, tgt=%s)", path, s, t));
      return;
    }

    BsonType sType = s.getBsonType();
    BsonType tType = t.getBsonType();

    if (isNumericType(sType) && isNumericType(tType)) {
      compareNumericValues(path, s, t, diffs);
      return;
    }

    if (sType != tType) {
      addDiff(
          diffs,
          String.format(
              "%s: BSON type mismatch (src=%s [%s], tgt=%s [%s])", path, s, sType, t, tType));
      return;
    }

    switch (sType) {
      case NULL:
      case UNDEFINED:
      case MIN_KEY:
      case MAX_KEY:
        return;

      case BOOLEAN:
        if (s.asBoolean().getValue() != t.asBoolean().getValue()) {
          addDiff(
              diffs,
              String.format(
                  "%s: boolean mismatch (src=%s, tgt=%s)",
                  path, s.asBoolean().getValue(), t.asBoolean().getValue()));
        }
        return;

      case STRING:
        if (!s.asString().getValue().equals(t.asString().getValue())) {
          addDiff(
              diffs,
              String.format(
                  "%s: string mismatch (src=%s, tgt=%s)",
                  path, quoteShort(s.asString().getValue()), quoteShort(t.asString().getValue())));
        }
        return;

      case SYMBOL:
        if (!s.asSymbol().getSymbol().equals(t.asSymbol().getSymbol())) {
          addDiff(
              diffs,
              String.format(
                  "%s: symbol mismatch (src=%s, tgt=%s)",
                  path, s.asSymbol().getSymbol(), t.asSymbol().getSymbol()));
        }
        return;

      case OBJECT_ID:
        if (!s.asObjectId().getValue().equals(t.asObjectId().getValue())) {
          addDiff(
              diffs,
              String.format(
                  "%s: ObjectId mismatch (src=%s, tgt=%s)",
                  path,
                  s.asObjectId().getValue().toHexString(),
                  t.asObjectId().getValue().toHexString()));
        }
        return;

      case DATE_TIME:
        if (s.asDateTime().getValue() != t.asDateTime().getValue()) {
          addDiff(
              diffs,
              String.format(
                  "%s: DateTime mismatch (src=%d, tgt=%d)",
                  path, s.asDateTime().getValue(), t.asDateTime().getValue()));
        }
        return;

      case TIMESTAMP:
        BsonTimestamp tsS = s.asTimestamp();
        BsonTimestamp tsT = t.asTimestamp();
        if (tsS.getTime() != tsT.getTime() || tsS.getInc() != tsT.getInc()) {
          addDiff(
              diffs,
              String.format(
                  "%s: BSON Timestamp mismatch (src=(%d,%d), tgt=(%d,%d))",
                  path, tsS.getTime(), tsS.getInc(), tsT.getTime(), tsT.getInc()));
        }
        return;

      case BINARY:
        BsonBinary binS = s.asBinary();
        BsonBinary binT = t.asBinary();
        if (binS.getType() != binT.getType()) {
          addDiff(
              diffs,
              String.format(
                  "%s: Binary subtype mismatch (src=0x%02x, tgt=0x%02x)",
                  path, Byte.toUnsignedInt(binS.getType()), Byte.toUnsignedInt(binT.getType())));
          return;
        }
        if (!Arrays.equals(binS.getData(), binT.getData())) {
          addDiff(
              diffs,
              String.format(
                  "%s: Binary payload mismatch (len src=%d, tgt=%d)",
                  path, binS.getData().length, binT.getData().length));
        }
        return;

      case REGULAR_EXPRESSION:
        BsonRegularExpression reS = s.asRegularExpression();
        BsonRegularExpression reT = t.asRegularExpression();
        if (!reS.getPattern().equals(reT.getPattern())
            || !canonicalizeRegexOptions(reS.getOptions())
                .equals(canonicalizeRegexOptions(reT.getOptions()))) {
          addDiff(
              diffs,
              String.format(
                  "%s: Regex mismatch (src=/%s/%s, tgt=/%s/%s)",
                  path, reS.getPattern(), reS.getOptions(), reT.getPattern(), reT.getOptions()));
        }
        return;

      case DOCUMENT:
        compareDocumentNodes(path, s.asDocument(), t.asDocument(), /* isRoot= */ false, diffs);
        return;

      case ARRAY:
        BsonArray arrS = s.asArray();
        BsonArray arrT = t.asArray();
        if (arrS.size() != arrT.size()) {
          addDiff(
              diffs,
              String.format(
                  "%s: array length mismatch (src=%d, tgt=%d)", path, arrS.size(), arrT.size()));
          return;
        }
        for (int i = 0; i < arrS.size(); i++) {
          compareValues(path + "[" + i + "]", arrS.get(i), arrT.get(i), diffs);
          if (diffs.size() >= config.getMaxDiffsPerDocument()) {
            return;
          }
        }
        return;

      default:
        if (!s.equals(t)) {
          addDiff(diffs, String.format("%s: value mismatch (src=%s, tgt=%s)", path, s, t));
        }
    }
  }

  private void compareNumericValues(String path, BsonValue s, BsonValue t, List<String> diffs) {
    BsonType sType = s.getBsonType();
    BsonType tType = t.getBsonType();

    if (config.isStrictTypeChecking() && sType != tType) {
      addDiff(
          diffs,
          String.format(
              "%s: numeric BSON type mismatch under strict mode (src=%s [%s], tgt=%s [%s])",
              path, s, sType, t, tType));
      return;
    }

    boolean nanS = isNumericNaN(s);
    boolean nanT = isNumericNaN(t);
    if (nanS || nanT) {
      if (!(nanS && nanT)) {
        addDiff(diffs, String.format("%s: NaN mismatch (src=%s, tgt=%s)", path, s, t));
      }
      return;
    }

    int infS = numericInfinitySign(s);
    int infT = numericInfinitySign(t);
    if (infS != 0 || infT != 0) {
      if (infS != infT) {
        addDiff(diffs, String.format("%s: Infinity mismatch (src=%s, tgt=%s)", path, s, t));
      }
      return;
    }

    if (isExactInteger(sType) && isExactInteger(tType)) {
      long ls = asLong(s);
      long lt = asLong(t);
      if (ls != lt) {
        addDiff(diffs, String.format("%s: integer mismatch (src=%d, tgt=%d)", path, ls, lt));
      }
      return;
    }

    if (sType == BsonType.DECIMAL128 && tType == BsonType.DECIMAL128) {
      Decimal128 decS = s.asDecimal128().getValue();
      Decimal128 decT = t.asDecimal128().getValue();
      if (config.isStrictTypeChecking()
          ? !decS.equals(decT)
          : decS.bigDecimalValue().compareTo(decT.bigDecimalValue()) != 0) {
        addDiff(diffs, String.format("%s: Decimal128 mismatch (src=%s, tgt=%s)", path, decS, decT));
      }
      return;
    }

    if (sType == BsonType.DOUBLE && tType == BsonType.DOUBLE) {
      double ds = s.asDouble().getValue();
      double dt = t.asDouble().getValue();
      if (Math.abs(ds - dt) > NUMERIC_TOLERANCE) {
        addDiff(diffs, String.format("%s: double mismatch (src=%s, tgt=%s)", path, ds, dt));
      }
      return;
    }

    // Relaxed cross-numeric comparison (strictTypeChecking == false): guard 64-bit integers > 2^53
    // against silent float64 rounding precision loss.
    if (isExactInteger(sType) && tType == BsonType.DOUBLE) {
      long ls = asLong(s);
      if (ls > MAX_EXACT_FLOAT_INT || ls < -MAX_EXACT_FLOAT_INT) {
        addDiff(
            diffs,
            String.format(
                "%s: int64 precision loss against double outside +-2^53 (src=%d, tgt=%s)",
                path, ls, t));
        return;
      }
    } else if (sType == BsonType.DOUBLE && isExactInteger(tType)) {
      long lt = asLong(t);
      if (lt > MAX_EXACT_FLOAT_INT || lt < -MAX_EXACT_FLOAT_INT) {
        addDiff(
            diffs,
            String.format(
                "%s: int64 precision loss against double outside +-2^53 (src=%s, tgt=%d)",
                path, s, lt));
        return;
      }
    }

    BigDecimal bdS = toBigDecimalExact(s);
    BigDecimal bdT = toBigDecimalExact(t);
    if (bdS.subtract(bdT).abs().compareTo(BigDecimal.valueOf(NUMERIC_TOLERANCE)) > 0) {
      addDiff(diffs, String.format("%s: numeric mismatch (src=%s, tgt=%s)", path, s, t));
    }
  }

  private boolean isIgnored(String path) {
    return config.getIgnoredFieldPaths().contains(path);
  }

  private void addDiff(List<String> diffs, String message) {
    if (diffs.size() < config.getMaxDiffsPerDocument()) {
      diffs.add(message);
    }
  }

  private static boolean isNumericType(BsonType type) {
    return type == BsonType.INT32
        || type == BsonType.INT64
        || type == BsonType.DOUBLE
        || type == BsonType.DECIMAL128;
  }

  private static boolean isExactInteger(BsonType type) {
    return type == BsonType.INT32 || type == BsonType.INT64;
  }

  private static long asLong(BsonValue v) {
    return v.getBsonType() == BsonType.INT32 ? v.asInt32().getValue() : v.asInt64().getValue();
  }

  private static boolean isNumericNaN(BsonValue v) {
    if (v.getBsonType() == BsonType.DOUBLE) {
      return Double.isNaN(v.asDouble().getValue());
    }
    if (v.getBsonType() == BsonType.DECIMAL128) {
      return v.asDecimal128().getValue().isNaN();
    }
    return false;
  }

  private static int numericInfinitySign(BsonValue v) {
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

  private static BigDecimal toBigDecimalExact(BsonValue v) {
    switch (v.getBsonType()) {
      case INT32:
        return BigDecimal.valueOf(v.asInt32().getValue());
      case INT64:
        return BigDecimal.valueOf(v.asInt64().getValue());
      case DOUBLE:
        double d = v.asDouble().getValue();
        return d == 0.0d ? BigDecimal.ZERO : BigDecimal.valueOf(d);
      case DECIMAL128:
        return v.asDecimal128().getValue().bigDecimalValue();
      default:
        throw new IllegalArgumentException("Unexpected numeric BsonType: " + v.getBsonType());
    }
  }

  private static String canonicalizeRegexOptions(String options) {
    if (options == null || options.isEmpty()) {
      return "";
    }
    char[] chars = options.toCharArray();
    Arrays.sort(chars);
    return new String(chars);
  }

  private static String quoteShort(String val) {
    if (val.length() <= 64) {
      return "\"" + val + "\"";
    }
    return "\"" + val.substring(0, 61) + "...\"";
  }
}
