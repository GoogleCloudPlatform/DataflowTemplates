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
import com.google.common.annotations.VisibleForTesting;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class ProcessChangeEventFn
    extends DoFn<MongoDbChangeEventContext, MongoDbChangeEventContext> {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessChangeEventFn.class);
  private String connectionString = "";
  private String targetDatabaseName = "";
  private MongoClient client = null;

  public static TupleTag<MongoDbChangeEventContext> successfulWriteTag =
      new TupleTag<>("successfulWrite");
  public static TupleTag<MongoDbChangeEventContext> failedWriteTag = new TupleTag<>("failedWrite");

  public ProcessChangeEventFn(String connectionString, String databaseName) {
    this.connectionString = connectionString;
    this.targetDatabaseName = databaseName;
  }

  @VisibleForTesting
  public ProcessChangeEventFn(MongoClient mongoClient, String databaseName) {
    this.client = mongoClient;
    this.targetDatabaseName = databaseName;
  }

  @ProcessElement
  public void processElement(ProcessContext context, MultiOutputReceiver out) {
    MongoDbChangeEventContext element = context.element();
    ClientSession session = null;

    try {
      MongoDatabase database = client.getDatabase(targetDatabaseName);
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
      Bson lookupById = eq("_id", docId);

      session = client.startSession();
      session.startTransaction();
      LOG.info("Started transaction for document ID: {}", element.getDocumentId());

      LOG.info("Querying shadow collection for document ID: {}", docId);
      Document shadowDoc = shadowCollection.find(session, lookupById).first();
      LOG.info("Shadow document found: {}", shadowDoc != null ? "yes" : "no");

      if (isEventNewerThanShadowDoc(element, shadowDoc)) {
        LOG.info("Processing newer event for document ID: {}", docId);

        if (element.isDeleteEvent()) {
          // This is a delete event - delete the document from data collection
          LOG.info("Deleting document with ID: {}", docId);
          dataCollection.deleteOne(session, lookupById);
          // Update the shadow document to record this deletion event
          shadowCollection.replaceOne(
              session, lookupById, element.getShadowDocument(), new ReplaceOptions().upsert(true));
          LOG.info("Updated shadow document for delete event, document ID: {}", docId);
        } else {
          // Regular insert or update.
          LOG.info(
              "Updating document with ID {} with data {}", docId, element.getDataAsJsonString());
          dataCollection.replaceOne(
              session,
              lookupById,
              Utils.jsonToDocument(element.getDataAsJsonString(), element.getDocumentId()),
              new ReplaceOptions().upsert(true));
          shadowCollection.replaceOne(
              session, lookupById, element.getShadowDocument(), new ReplaceOptions().upsert(true));
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
    }
  }

  @Setup
  public void setup() {
    if (client == null) {
      // Log the projectId before creating the client
      LOG.info(
          "Creating MongoDB client in ProcessChangeEventFn with connection string: {}",
          connectionString);
      MongoClientSettings settings =
          MongoClientSettings.builder()
              .applyConnectionString(new com.mongodb.ConnectionString(connectionString))
              .uuidRepresentation(UuidRepresentation.STANDARD)
              .build();
      client = MongoClients.create(settings);
    }
  }

  @Teardown
  public void teardown() {
    // Close the MongoClient when the pipeline finishes
    if (client != null) {
      client.close();
    }
  }

  private static boolean isEventNewerThanShadowDoc(
      MongoDbChangeEventContext event, Document shadowDoc) {
    return shadowDoc == null
        || Utils.isNewerTimestamp(
            event.getTimestampDoc(),
            (Document) shadowDoc.get(MongoDbChangeEventContext.TIMESTAMP_COL));
  }
}
