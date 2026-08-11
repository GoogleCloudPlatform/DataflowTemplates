/*
 * Copyright (C) 2025 Google LLC
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

import static com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext.DATA_COL;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utils used by the Datastream-mongodb-to-firestore pipeline. */
public final class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  private static final JsonWriterSettings CANONICAL_JSON_SETTINGS =
      JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

  private Utils() {}

  public static void removeTableRowFields(Document doc, Set<String> ignoreFields) {
    for (String ignoreField : ignoreFields) {
      doc.remove(ignoreField);
    }
  }

  /* Whether the first timestamp is later than the second timestamp. */
  public static boolean isNewerTimestamp(Document ts1, Document ts2) {
    if (ts1 == null) {
      return false;
    }
    if (ts2 == null) {
      return true;
    }
    long s1 = 0L;
    int n1 = 0;
    if (ts1.containsKey(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL)) {
      Object s = ts1.get(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL);
      if (s instanceof Number) {
        s1 = ((Number) s).longValue();
      }
    }
    if (ts1.containsKey(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL)) {
      Object n = ts1.get(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL);
      if (n instanceof Number) {
        n1 = ((Number) n).intValue();
      }
    }
    long s2 = 0L;
    int n2 = 0;
    if (ts2.containsKey(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL)) {
      Object s = ts2.get(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL);
      if (s instanceof Number) {
        s2 = ((Number) s).longValue();
      }
    }
    if (ts2.containsKey(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL)) {
      Object n = ts2.get(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL);
      if (n instanceof Number) {
        n2 = ((Number) n).intValue();
      }
    }
    return s1 > s2 || (s1 == s2 && n1 > n2);
  }

  public static long getTimestampNanos(Document timestampDoc) {
    if (timestampDoc == null) {
      return 0L;
    }
    long seconds = 0L;
    if (timestampDoc.containsKey(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL)) {
      Object s = timestampDoc.get(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL);
      if (s instanceof Number) {
        seconds = ((Number) s).longValue();
      }
    }
    long nanos = 0L;
    if (timestampDoc.containsKey(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL)) {
      Object n = timestampDoc.get(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL);
      if (n instanceof Number) {
        nanos = ((Number) n).longValue();
      }
    }
    return (seconds * 1_000_000_000L) + nanos;
  }

  public static Document jsonToDocument(String jsonString, Object documentId) {
    if (jsonString == null) {
      return null;
    }
    Document rawDoc = null;
    try {
      Document parsed = Document.parse(jsonString);
      if (parsed.containsKey(DATA_COL)) {
        Object dataObj = parsed.get(DATA_COL);
        if (dataObj instanceof Document) {
          rawDoc = (Document) dataObj;
        } else if (dataObj instanceof String) {
          rawDoc = Document.parse((String) dataObj);
        } else if (dataObj != null) {
          rawDoc = Document.parse(dataObj.toString());
        }
      } else {
        // No 'data' wrapper field; the parsed document itself is the payload
        rawDoc = parsed;
      }
    } catch (Exception ex) {
      LOG.debug("Document parsing for {} failed due to {}.", jsonString, ex.getMessage());
    }
    if (rawDoc == null) {
      return null;
    }
    removeTableRowFields(
        rawDoc,
        com.google.cloud.teleport.v2.templates.DataStreamMongoDBToFirestore.MAPPER_IGNORE_FIELDS);
    rawDoc.put(MongoDbChangeEventContext.DOC_ID_COL, documentId);
    return rawDoc;
  }

  /**
   * Converts a MongoDB document ID into a type-tagged, collision-free string representation.
   *
   * <p><b>NOTE:</b> This method does NOT generate a semantically equivalent string for database
   * writes, and must NEVER be used as the destination document's {@code _id} value (which should
   * retain native BSON types such as {@link org.bson.types.ObjectId}, {@link Document}, or {@link
   * org.bson.types.Binary}).
   *
   * <p><b>Use Case:</b> This method is strictly intended for generating internal Apache Beam
   * grouping and shuffling keys (e.g. {@code collection + "#" + documentIdToString(docId)}) and
   * diagnostic string logs. The type prefix (e.g. {@code str_}, {@code i64_}, {@code bin_<type>_})
   * ensures distinct BSON types with identical string forms (such as string {@code "123"} vs Long
   * {@code 123L}) never collide in Beam's stateful deduplication and windowing operations.
   *
   * @param documentId the raw BSON document ID object
   * @return a type-tagged string representation suitable for pipeline routing keys
   */
  public static String documentIdToString(Object documentId) {
    if (documentId == null) {
      return "null";
    }
    if (documentId instanceof Binary) {
      Binary binary = (Binary) documentId;
      return "bin_" + binary.getType() + "_" + Base64.getEncoder().encodeToString(binary.getData());
    }
    if (documentId instanceof Document) {
      return "doc_" + ((Document) documentId).toJson(CANONICAL_JSON_SETTINGS);
    }
    if (documentId instanceof List) {
      Document wrapper = new Document("arr", documentId);
      return "list_" + wrapper.toJson(CANONICAL_JSON_SETTINGS);
    }
    if (documentId instanceof ObjectId) {
      return "oid_" + ((ObjectId) documentId).toHexString();
    }
    if (documentId instanceof Long) {
      return "i64_" + documentId;
    }
    if (documentId instanceof Integer) {
      return "i32_" + documentId;
    }
    if (documentId instanceof Double) {
      return "f64_" + documentId;
    }
    if (documentId instanceof Boolean) {
      return "bool_" + documentId;
    }
    return "str_" + documentId.toString();
  }

  public static String getCanonicalJsonOfDataField(Document fullEvent) {
    Object dataVal = fullEvent.get(DATA_COL);
    if (dataVal == null) {
      return null;
    }
    if (dataVal instanceof Document) {
      return ((Document) dataVal).toJson(CANONICAL_JSON_SETTINGS);
    } else if (dataVal instanceof String) {
      Document dataDoc = Document.parse((String) dataVal);
      return dataDoc.toJson(CANONICAL_JSON_SETTINGS);
    }
    throw new IllegalArgumentException("Unsupported data field type: " + dataVal.getClass());
  }

  public static String getCanonicalJsonOfDataField(String jsonString) {
    return getCanonicalJsonOfDataField(Document.parse(jsonString));
  }

  public static Document extractInnerEvent(Document doc) {
    return doc.containsKey(DatastreamConstants.CHANGE_EVENT)
        ? (Document) doc.get(DatastreamConstants.CHANGE_EVENT)
        : doc;
  }

  public static JsonNode extractInnerEvent(JsonNode payload) {
    return payload.has(DatastreamConstants.CHANGE_EVENT)
        ? payload.get(DatastreamConstants.CHANGE_EVENT)
        : payload;
  }
}
