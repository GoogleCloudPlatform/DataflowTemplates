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
package com.google.cloud.teleport.v2.mongodb.validator.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

/** Canonical BSON wire encoding, Extended JSON formatting, and raw duplicate-key inspection. */
public final class BsonEncodingUtils {

  private static final BsonDocumentCodec DOCUMENT_CODEC = new BsonDocumentCodec();
  private static final JsonWriterSettings EXTENDED_JSON =
      JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

  private BsonEncodingUtils() {}

  /**
   * Encodes a single {@link BsonValue} as canonical BSON bytes wrapped in {@code {"_id": value}},
   * preserving exact BSON type tags and nested field ordering for collision-free map keys.
   */
  public static byte[] encodeIdBytes(BsonValue idValue) {
    BsonDocument wrapper = new BsonDocument("_id", idValue);
    BasicOutputBuffer buffer = new BasicOutputBuffer(64);
    try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
      DOCUMENT_CODEC.encode(writer, wrapper, EncoderContext.builder().build());
    }
    return buffer.toByteArray();
  }

  /** Decodes a {@link BsonValue} previously serialized with {@link #encodeIdBytes(BsonValue)}. */
  public static BsonValue decodeIdBytes(byte[] wrapperBytes) {
    RawBsonDocument raw = new RawBsonDocument(wrapperBytes);
    return raw.get("_id");
  }

  /** Renders a {@link BsonValue} as canonical Extended JSON (e.g. {@code {"$oid": "..."}}). */
  public static String toExtendedJson(BsonValue idValue) {
    if (idValue == null) {
      return "null";
    }
    BsonDocument wrapper = new BsonDocument("_id", idValue);
    String fullJson = wrapper.toJson(EXTENDED_JSON);
    BsonDocument parsed = BsonDocument.parse(fullJson);
    // Extract just the canonical value representation when possible
    int colonIdx = fullJson.indexOf(':');
    int endBrace = fullJson.lastIndexOf('}');
    if (colonIdx > 0 && endBrace > colonIdx && parsed.containsKey("_id")) {
      return fullJson.substring(colonIdx + 1, endBrace).trim();
    }
    return fullJson;
  }

  /** Estimates the serialized BSON size in bytes of a {@link BsonDocument}. */
  public static int estimateBytes(BsonDocument doc) {
    if (doc instanceof RawBsonDocument) {
      return ((RawBsonDocument) doc).getByteBuffer().remaining();
    }
    BasicOutputBuffer buffer = new BasicOutputBuffer(256);
    try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
      DOCUMENT_CODEC.encode(writer, doc, EncoderContext.builder().build());
    }
    return buffer.size();
  }

  /** Materializes a {@link BsonDocument} into a decoded tree for fast repeated traversal. */
  public static BsonDocument materialize(BsonDocument doc) {
    if (doc instanceof RawBsonDocument) {
      ByteBuffer nioBuf = ((RawBsonDocument) doc).getByteBuffer().asNIO();
      try (BsonBinaryReader reader = new BsonBinaryReader(nioBuf)) {
        return DOCUMENT_CODEC.decode(reader, DecoderContext.builder().build());
      }
    }
    return doc;
  }

  /**
   * Walks raw BSON wire bytes to detect duplicate field names within the same document or nested
   * subdocument before standard map-backed decoding overwrites them.
   */
  public static List<String> findDuplicateKeys(RawBsonDocument rawDoc) {
    List<String> duplicates = new ArrayList<>();
    ByteBuffer nioBuf = rawDoc.getByteBuffer().asNIO();
    try (BsonBinaryReader reader = new BsonBinaryReader(nioBuf)) {
      inspectDocumentDuplicates(reader, "", duplicates);
    }
    return duplicates;
  }

  private static void inspectDocumentDuplicates(
      BsonBinaryReader reader, String prefix, List<String> duplicates) {
    reader.readStartDocument();
    Set<String> seen = new HashSet<>();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      BsonType type = reader.getCurrentBsonType();
      String name = reader.readName();
      String path = prefix.isEmpty() ? name : prefix + "." + name;
      if (!seen.add(name)) {
        duplicates.add(path);
      }
      skipOrRecurseValue(reader, type, path, duplicates);
    }
    reader.readEndDocument();
  }

  private static void inspectArrayDuplicates(
      BsonBinaryReader reader, String prefix, List<String> duplicates) {
    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      BsonType type = reader.getCurrentBsonType();
      String indexName = reader.readName();
      String path = prefix + "[" + indexName + "]";
      skipOrRecurseValue(reader, type, path, duplicates);
    }
    reader.readEndDocument();
  }

  private static void skipOrRecurseValue(
      BsonBinaryReader reader, BsonType type, String path, List<String> duplicates) {
    if (type == BsonType.DOCUMENT) {
      inspectDocumentDuplicates(reader, path, duplicates);
    } else if (type == BsonType.ARRAY) {
      inspectArrayDuplicates(reader, path, duplicates);
    } else {
      reader.skipValue();
    }
  }
}
