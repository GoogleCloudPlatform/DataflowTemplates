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

import static com.mongodb.client.model.Filters.eq;

import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class ProcessChangeEventFn
    extends DoFn<MongoDbChangeEventContext, MongoDbChangeEventContext> {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessChangeEventFn.class);
  private final String connectionString;
  private final String targetDatabaseName;

  public static TupleTag<MongoDbChangeEventContext> successfulWriteTag =
      new TupleTag<>("successfulWrite");
  public static TupleTag<MongoDbChangeEventContext> failedWriteTag = new TupleTag<>("failedWrite");

  public ProcessChangeEventFn(String connectionString, String databaseName) {
    this.connectionString = connectionString;
    this.targetDatabaseName = databaseName;
  }

  @ProcessElement
  public void processElement(ProcessContext context, MultiOutputReceiver out) {
    MongoDbChangeEventContext element = context.element();

    // Log the projectId before creating the client
    LOG.info(
        "Creating MongoDB client in ProcessChangeEventFn with connection string: {}",
        connectionString);

    MongoClient client = null;
    ClientSession session = null;

    try {
      client = MongoClients.create(connectionString);
      MongoDatabase database = client.getDatabase(targetDatabaseName);
      session = client.startSession();

      session.startTransaction();
      LOG.info("Started transaction for document ID: {}", element.getDocumentId());

      MongoCollection<Document> dataCollection =
          database.getCollection(element.getDataCollection());
      MongoCollection<Document> shadowCollection =
          database.getCollection(element.getShadowCollection());

      LOG.info(
          "Accessing collections - data: {}, shadow: {}",
          element.getDataCollection(),
          element.getShadowCollection());

      // Step 1: Query the shadow collection to see if there is any existing record of this id
      Object docId = element.getDocumentId();
      LOG.info("Querying shadow collection for document ID: {}", docId);
      Document shadowDoc =
          shadowCollection
              .find(session, eq(MongoDbChangeEventContext.SHADOW_DOC_ID_COL, docId))
              .first();
      LOG.info("Shadow document found: {}", shadowDoc != null ? "yes" : "no");

      if (shadowDoc == null
          || Utils.isNewerTimestamp(
              element.getTimestampDoc(),
              (Document) shadowDoc.get(MongoDbChangeEventContext.TIMESTAMP_COL))) {
        // Incoming event is newer
        LOG.info("Processing newer event for document ID: {}", docId);
        if (element.isDeleteEvent()) {
          // This is a delete event - delete the document from data collection
          LOG.info("Deleting document with ID: {}", docId);
          dataCollection.deleteOne(session, eq(MongoDbChangeEventContext.DOC_ID_COL, docId));
          // Update the shadow document to record this deletion event
          shadowCollection.replaceOne(
              session,
              eq(MongoDbChangeEventContext.SHADOW_DOC_ID_COL, docId),
              element.getShadowDocument(),
              new ReplaceOptions().upsert(true));
          LOG.info("Updated shadow document for delete event, document ID: {}", docId);
        } else {
          // Regular insert or update.
          LOG.info("Updating document with ID {} with data {}", docId, element.getDataDocument());
          dataCollection.replaceOne(
              session,
              eq(MongoDbChangeEventContext.DOC_ID_COL, docId),
              element.getDataDocument(),
              new ReplaceOptions().upsert(true));
          shadowCollection.replaceOne(
              session,
              eq(MongoDbChangeEventContext.SHADOW_DOC_ID_COL, docId),
              element.getShadowDocument(),
              new ReplaceOptions().upsert(true));
          LOG.info("Updated document and shadow document, document ID: {}", docId);
        }
      } else {
        // Existing document has a later timestamp, skip this event
        LOG.info("Skipping event for document ID: {} as a newer version exists", docId);
      }
      session.commitTransaction();
      LOG.info("Transaction committed for document ID: {}", docId);
      out.get(successfulWriteTag).output(element);
      LOG.info("Successfully processed document ID: {}", element.getDocumentId());
    } catch (Exception e) {
      LOG.error(
          "Transaction failed for document ID: {}: {}", element.getDocumentId(), e.getMessage(), e);
      if (session != null) {
        session.abortTransaction();
        LOG.info("Transaction aborted for document ID: {}", element.getDocumentId());
      }
      out.get(failedWriteTag).output(element);
    } finally {
      if (session != null) {
        session.close();
      }
      if (client != null) {
        client.close();
      }
    }
  }
}
