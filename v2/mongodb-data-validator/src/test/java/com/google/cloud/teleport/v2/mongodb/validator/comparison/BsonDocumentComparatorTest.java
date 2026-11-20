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

import static com.google.common.truth.Truth.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Decimal128;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BsonDocumentComparator}. */
@RunWith(JUnit4.class)
public class BsonDocumentComparatorTest {

  @Test
  public void strictMode_rejectsNumericTypeDrift_whileRelaxedAllowsSafeNumbers() {
    BsonDocument src = new BsonDocument("_id", new BsonInt32(1)).append("count", new BsonInt32(42));
    BsonDocument tgt =
        new BsonDocument("_id", new BsonInt32(1)).append("count", new BsonDouble(42.0));

    BsonDocumentComparator strict =
        new BsonDocumentComparator(BsonDocumentComparator.Config.strictDefault());
    assertThat(strict.compare(src, tgt)).hasSize(1);
    assertThat(strict.compare(src, tgt).get(0)).contains("numeric BSON type mismatch");

    BsonDocumentComparator relaxed =
        new BsonDocumentComparator(
            new BsonDocumentComparator.Config(false, false, Collections.emptySet(), 10));
    assertThat(relaxed.compare(src, tgt)).isEmpty();
  }

  @Test
  public void relaxedMode_stillRejectsInt64PrecisionLossAbove2Pow53() {
    long exactInt64 = (1L << 53) + 1L;
    double roundedDouble = (double) exactInt64; // Rounds to 2^53 in IEEE-754!

    BsonDocument src = new BsonDocument("big", new BsonInt64(exactInt64));
    BsonDocument tgt = new BsonDocument("big", new BsonDouble(roundedDouble));

    BsonDocumentComparator relaxed =
        new BsonDocumentComparator(
            new BsonDocumentComparator.Config(false, false, Collections.emptySet(), 10));
    List<String> diffs = relaxed.compare(src, tgt);
    assertThat(diffs).hasSize(1);
    assertThat(diffs.get(0)).contains("precision loss");
  }

  @Test
  public void nanHandling_doesNotSilentlyMatchFiniteNumbers() {
    BsonDocument nanDoc = new BsonDocument("v", new BsonDouble(Double.NaN));
    BsonDocument zeroDoc = new BsonDocument("v", new BsonDouble(0.0));
    BsonDocument nanDoc2 = new BsonDocument("v", new BsonDouble(Double.NaN));

    BsonDocumentComparator cmp =
        new BsonDocumentComparator(
            new BsonDocumentComparator.Config(false, false, Collections.emptySet(), 10));
    assertThat(cmp.compare(nanDoc, zeroDoc)).hasSize(1);
    assertThat(cmp.compare(nanDoc, nanDoc2)).isEmpty();
  }

  @Test
  public void binarySubtypeAndNullUndefinedAndTimestampDistinguished() {
    byte[] bytes = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    BsonDocument src =
        new BsonDocument("uuid", new BsonBinary(BsonBinarySubType.UUID_STANDARD, bytes))
            .append("opt", BsonNull.VALUE)
            .append("ts", new BsonTimestamp(1700000000, 1))
            .append("strBool", new BsonString("true"))
            .append("dec", new BsonDecimal128(Decimal128.parse("12.34")));

    BsonDocument tgt =
        new BsonDocument("uuid", new BsonBinary(BsonBinarySubType.UUID_LEGACY, bytes))
            .append("opt", new BsonUndefined())
            .append("ts", new BsonDateTime(1700000000000L))
            .append("strBool", org.bson.BsonBoolean.TRUE)
            .append("dec", new BsonString("12.34"));

    BsonDocumentComparator cmp =
        new BsonDocumentComparator(BsonDocumentComparator.Config.strictDefault());
    List<String> diffs = cmp.compare(src, tgt);
    assertThat(diffs).hasSize(5);
  }

  @Test
  public void subdocumentKeyOrder_enforcedByDefaultAndConfigurable() {
    BsonDocument subA =
        new BsonDocument("first", new BsonInt32(1)).append("second", new BsonInt32(2));
    BsonDocument subB =
        new BsonDocument("second", new BsonInt32(2)).append("first", new BsonInt32(1));
    BsonDocument src = new BsonDocument("nested", subA);
    BsonDocument tgt = new BsonDocument("nested", subB);

    BsonDocumentComparator strictOrder =
        new BsonDocumentComparator(BsonDocumentComparator.Config.strictDefault());
    assertThat(strictOrder.compare(src, tgt)).hasSize(1);
    assertThat(strictOrder.compare(src, tgt).get(0)).contains("key order mismatch");

    BsonDocumentComparator ignoreOrder =
        new BsonDocumentComparator(
            new BsonDocumentComparator.Config(true, true, Collections.emptySet(), 10));
    assertThat(ignoreOrder.compare(src, tgt)).isEmpty();
  }

  @Test
  public void rawDuplicateKeysAndIgnoredFieldsHandled() {
    BasicOutputBuffer buf = new BasicOutputBuffer();
    try (BsonBinaryWriter writer = new BsonBinaryWriter(buf)) {
      writer.writeStartDocument();
      writer.writeInt32("dup", 1);
      writer.writeInt32("dup", 2);
      writer.writeEndDocument();
    }
    RawBsonDocument rawDup = new RawBsonDocument(buf.toByteArray());
    BsonDocument clean = new BsonDocument("dup", new BsonInt32(2));

    BsonDocumentComparator cmp =
        new BsonDocumentComparator(BsonDocumentComparator.Config.strictDefault());
    List<String> diffs = cmp.compare(rawDup, clean);
    assertThat(diffs).isNotEmpty();
    assertThat(diffs.get(0)).contains("duplicate BSON key");

    // Ignored fields excluded cleanly
    Set<String> ignored =
        BsonDocumentComparator.Config.parseIgnoredFields("_migrated_at, meta.syncTs");
    BsonDocumentComparator withIgnored =
        new BsonDocumentComparator(new BsonDocumentComparator.Config(true, false, ignored, 10));
    BsonDocument s2 =
        BsonDocument.parse(
            "{\"_id\": 1, \"name\": \"alice\", \"meta\": {\"v\": 1, \"syncTs\": 10}}");
    BsonDocument t2 =
        BsonDocument.parse(
            "{\"_id\": 1, \"name\": \"alice\", \"_migrated_at\": 99, \"meta\": {\"v\": 1, \"syncTs\": 20}}");
    assertThat(withIgnored.compare(s2, t2)).isEmpty();
  }

  @Test
  public void rawDocumentsWithArrays_compareCleanAndDetectDuplicatesInsideArrayElements() {
    // Regression: the raw duplicate-key walk used to read arrays with the document API and threw
    // "readStartDocument can only be called when CurrentBSONType is DOCUMENT" on any array field.
    BsonDocument withArrays =
        BsonDocument.parse(
            "{\"_id\": \"order_00001\", \"tags\": [\"tier-1\", \"v1\"],"
                + " \"matrix\": [[1, 2], [3]], \"items\": [{\"sku\": \"a\", \"qty\": 1}]}");
    RawBsonDocument rawSrc = RawBsonDocument.parse(withArrays.toJson());
    RawBsonDocument rawTgt = RawBsonDocument.parse(withArrays.toJson());

    BsonDocumentComparator cmp =
        new BsonDocumentComparator(BsonDocumentComparator.Config.strictDefault());
    assertThat(cmp.compare(rawSrc, rawTgt)).isEmpty();

    // A duplicate key inside a subdocument that is itself an array element is still reported.
    BasicOutputBuffer buf = new BasicOutputBuffer();
    try (BsonBinaryWriter writer = new BsonBinaryWriter(buf)) {
      writer.writeStartDocument();
      writer.writeStartArray("items");
      writer.writeString("scalar");
      writer.writeStartDocument();
      writer.writeInt32("dup", 1);
      writer.writeInt32("dup", 2);
      writer.writeEndDocument();
      writer.writeEndArray();
      writer.writeEndDocument();
    }
    RawBsonDocument rawDupInArray = new RawBsonDocument(buf.toByteArray());
    BsonDocument clean = BsonDocument.parse("{\"items\": [\"scalar\", {\"dup\": 2}]}");

    List<String> diffs = cmp.compare(rawDupInArray, clean);
    assertThat(diffs).isNotEmpty();
    assertThat(diffs.get(0)).contains("duplicate BSON key 'items[1].dup'");
  }
}
