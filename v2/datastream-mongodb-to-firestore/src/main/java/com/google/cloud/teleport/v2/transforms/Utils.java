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

import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import java.util.Set;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utils used by the Datastream-mongodb-to-mongodb pipeline. */
public final class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  public static void removeTableRowFields(Document doc, Set<String> ignoreFields) {
    for (String ignoreField : ignoreFields) {
      doc.remove(ignoreField);
    }
  }

  /* Whether the first timestamp is later than the second timestamp. */
  public static boolean isNewerTimestamp(Document ts1, Document ts2) {
    long s1 = ts1.getLong(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL);
    int n1 = ts1.getInteger(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL);
    long s2 = ts2.getLong(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL);
    int n2 = ts2.getInteger(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL);
    return s1 > s2 || (s1 == s2 && n1 > n2);
  }

  public static Document jsonToDocument(String jsonString, Object documentId) {
    Document rawDoc;
    try {
      rawDoc = Document.parse(Document.parse(jsonString).get(DATA_COL).toString());
    } catch (Exception ex) {
      LOG.info(
          "Document parsing for {} failed due to {}, try casting.", jsonString, ex.getMessage());
      rawDoc = (Document) Document.parse(jsonString).get(DATA_COL);
    }
    rawDoc.put(MongoDbChangeEventContext.DOC_ID_COL, documentId);
    return rawDoc;
  }
}
