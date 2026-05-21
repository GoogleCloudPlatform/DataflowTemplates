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
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DoFn to process events with conflict resolving via transactions. */
public class ProcessChangeEventFn
    extends DoFn<MongoDbChangeEventContext, MongoDbChangeEventContext> {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessChangeEventFn.class);
  private String connectionString = "";
  private String targetDatabaseName = "";
  private MongoClient client = null;
  private final int maxRetries = 3; // Maximum number of retry attempts
  private final long retryDelayMs = 1000; // Initial delay in milliseconds

  public static TupleTag<MongoDbChangeEventContext> successfulWriteTag =
      new TupleTag<>("successfulWrite");
  public static TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
      failedWriteTag = new TupleTag<>("failedWrite");
  // Tag for severe failures that should not be retried (e.g. permanent errors or non-transient
  // transaction errors)
  public static TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
      severeFailedWriteTag = new TupleTag<>("severeFailedWrite");

  // Error code 2 corresponds to BadValue/InvalidArgument, which is treated as a permanent error.
  public static final int INVALID_ARGUMENT = 2;

  private final Counter successfulWrites =
      Metrics.counter(ProcessChangeEventFn.class, "successfulWrites");
  private final Counter retriableFailedWrites =
      Metrics.counter(ProcessChangeEventFn.class, "retriableFailedWrites");
  private final Counter severeFailedWrites =
      Metrics.counter(ProcessChangeEventFn.class, "severeFailedWrites");
  private final Counter dlqRetries = Metrics.counter(ProcessChangeEventFn.class, "dlqRetries");
  private final Counter totalProcessedDocuments =
      Metrics.counter(ProcessChangeEventFn.class, "totalProcessedDocuments");
  private final Counter retriedDocuments =
      Metrics.counter(ProcessChangeEventFn.class, "retriedDocuments");
  private final Counter inMemoryRetries =
      Metrics.counter(ProcessChangeEventFn.class, "inMemoryRetries");
  private final Counter outOfOrderSkips =
      Metrics.counter(ProcessChangeEventFn.class, "outOfOrderSkips");

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

    totalProcessedDocuments.inc();
    if (element.getRetryCount() > 0) {
      retriedDocuments.inc();
    }
    if (element.getIsDlqReconsumed()) {
      dlqRetries.inc();
    }

    int retryCount = 0;
    Exception lastException = null;
    while (retryCount <= maxRetries) {
      ClientSession session = null;
      try {
        MongoDatabase database = client.getDatabase(targetDatabaseName);
        MongoCollection<Document> dataCollection =
            database.getCollection(element.getDataCollection());
        MongoCollection<Document> shadowCollection =
            database.getCollection(element.getShadowCollection());

        // Step 1: Query the shadow collection to see if there is any existing record of this id
        Object docId = element.getDocumentId();
        Bson lookupById = eq("_id", docId);

        session = client.startSession();
        session.startTransaction();

        Document shadowDoc = shadowCollection.find(session, lookupById).first();

        if (isEventNewerThanShadowDoc(element, shadowDoc)) {
          if (element.isDeleteEvent()) {
            // This is a delete event - delete the document from data collection
            dataCollection.deleteOne(session, lookupById);
            // Update the shadow document to record this deletion event
            shadowCollection.replaceOne(
                session,
                lookupById,
                element.getShadowDocument(),
                new ReplaceOptions().upsert(true));
          } else {
            // Regular insert or update.
            dataCollection.replaceOne(
                session,
                lookupById,
                Utils.jsonToDocument(element.getDataAsJsonString(), element.getDocumentId()),
                new ReplaceOptions().upsert(true));
            shadowCollection.replaceOne(
                session,
                lookupById,
                element.getShadowDocument(),
                new ReplaceOptions().upsert(true));
          }
          successfulWrites.inc();
        } else {
          // Existing document has a later timestamp, skip this event
          outOfOrderSkips.inc();
        }
        session.commitTransaction();
        out.get(successfulWriteTag).output(element);
        break; // Exit the retry loop on success
      } catch (Exception e) {
        lastException = e;
        if (session != null && session.hasActiveTransaction()) {
          try {
            session.abortTransaction();
            LOG.warn(
                "Transaction aborted for document ID: {}, attempt: {}",
                element.getDocumentId(),
                retryCount + 1);
          } catch (MongoException abortException) {
            LOG.error(
                "Error aborting transaction for document ID: {}: {}",
                element.getDocumentId(),
                abortException.getMessage(),
                abortException);
          }
        }

        // Check if the error is permanent (e.g. code 2 for InvalidArgument when exceeding nesting
        // limit)
        boolean isPermanent = false;
        if (e instanceof MongoWriteException writeException) {
          if (writeException.getError().getCode() == INVALID_ARGUMENT) {
            isPermanent = true;
          }
        }

        // Check if the error is transient and safe to retry
        boolean isTransient = isTransientTransactionError(e);

        // If it's a permanent error or not transient, fail fast and route to severe DLQ
        if (isPermanent || !isTransient) {
          LOG.error(
              "Permanent or non-retryable error for document ID: {}: {}",
              element.getDocumentId(),
              e.getMessage());
          FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> failedElement =
              FailsafeElement.of(element, element);
          failedElement.setErrorMessage(e.getMessage());
          failedElement.setStacktrace(Throwables.getStackTraceAsString(e));
          out.get(severeFailedWriteTag).output(failedElement);

          String errorIdentifier = "UnknownError";
          if (e instanceof MongoWriteException writeException) {
            errorIdentifier = "MongoWriteException_" + writeException.getError().getCode();
          } else if (e instanceof com.mongodb.MongoCommandException commandException) {
            errorIdentifier = "MongoCommandException_" + commandException.getCode();
          } else if (e instanceof MongoException mongoException) {
            errorIdentifier = mongoException.getClass().getSimpleName();
          }
          Metrics.counter(ProcessChangeEventFn.class, "severeFailedWrites_" + errorIdentifier)
              .inc();

          severeFailedWrites.inc();
          break; // Exit the retry loop
        }

        long backoffMs = retryDelayMs * (1 << retryCount);
        if (retryCount < maxRetries) {
          inMemoryRetries.inc();

          String errorIdentifier = "UnknownError";
          if (e instanceof MongoWriteException writeException) {
            errorIdentifier = "MongoWriteException_" + writeException.getError().getCode();
          } else if (e instanceof com.mongodb.MongoCommandException commandException) {
            errorIdentifier = "MongoCommandException_" + commandException.getCode();
          } else if (e instanceof MongoException mongoException) {
            errorIdentifier = mongoException.getClass().getSimpleName();
          }
          Metrics.counter(ProcessChangeEventFn.class, "inMemoryRetries_" + errorIdentifier).inc();

          LOG.warn(
              "Transient transaction error encountered for document ID: {}, attempt: {}. Retrying"
                  + " in {} ms...",
              element.getDocumentId(),
              retryCount + 1,
              backoffMs,
              e);
          try {
            TimeUnit.MILLISECONDS.sleep(backoffMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.error("Retry sleep interrupted for document ID: {}", element.getDocumentId(), ie);
            FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> failedElement =
                FailsafeElement.of(element, element);
            failedElement.setErrorMessage(ie.getMessage());
            failedElement.setStacktrace(Throwables.getStackTraceAsString(ie));
            out.get(failedWriteTag).output(failedElement);
            retriableFailedWrites.inc();
            break; // Exit the retry loop if interrupted
          }
          retryCount++;
        } else {
          LOG.error(
              "Transaction failed after {} attempts for document ID: {}: {}",
              retryCount + 1,
              element.getDocumentId(),
              e.getMessage(),
              e);
          FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> failedElement =
              FailsafeElement.of(element, element);
          failedElement.setErrorMessage(e.getMessage());
          failedElement.setStacktrace(Throwables.getStackTraceAsString(e));
          out.get(failedWriteTag).output(failedElement);
          retriableFailedWrites.inc();
          LOG.info("Failed element of id {} sent to DLQ", element.getDocumentId());
          break; // Exit the retry loop on non-transient error or max retries
        }
      } finally {
        if (session != null) {
          session.close();
        }
      }
    }
    if (lastException != null && retryCount > maxRetries) {
      LOG.error(
          "Transaction failed after max retries ({}) for document ID: {}: {}",
          maxRetries,
          element.getDocumentId(),
          lastException.getMessage(),
          lastException);
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
              .applyToSocketSettings(
                  builder -> {
                    // How long the driver will wait to establish a connection
                    builder.connectTimeout(60, TimeUnit.SECONDS);
                    builder.readTimeout(60, TimeUnit.SECONDS); // Example: 60 seconds
                  })
              .applyToClusterSettings(
                  builder -> builder.serverSelectionTimeout(10, TimeUnit.MINUTES))
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

  public static boolean isTransientTransactionError(Exception e) {
    return e instanceof MongoException
        && ((MongoException) e).getErrorLabels().contains("TransientTransactionError");
  }
}
