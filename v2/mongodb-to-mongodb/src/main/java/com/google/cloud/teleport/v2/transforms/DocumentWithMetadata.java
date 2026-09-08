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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

/**
 * This class contains the raw document and metadata related to the migration. It is used to carry
 * the document, CDC operation type, ordering timestamp sort key, and DLQ retry context through the
 * pipeline without polluting the original document schema.
 */
@DefaultCoder(DocumentWithMetadataCoder.class)
public class DocumentWithMetadata implements Serializable {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final JsonWriterSettings CANONICAL_JSON_SETTINGS =
      JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

  public static final String METADATA_RETRY_COUNT = "_metadata_retry_count";
  public static final String METADATA_ERROR_TYPE = "_metadata_error_type";
  public static final String METADATA_SOURCE_COLLECTION = "_metadata_source_collection";
  public static final String METADATA_TARGET_COLLECTION = "_metadata_target_collection";
  public static final String METADATA_ERROR_MESSAGE = "_metadata_error_message";
  public static final String METADATA_FAILURE_STAGE = "_metadata_failure_stage";
  public static final String METADATA_OPERATION_TYPE = "_metadata_operation_type";
  public static final String METADATA_DOCUMENT_KEY = "_metadata_document_key";
  public static final String METADATA_TIMESTAMP_SECONDS = "_metadata_timestamp_seconds";
  public static final String METADATA_TIMESTAMP_SUB_SECONDS = "_metadata_timestamp_sub_seconds";
  public static final String METADATA_IS_CDC = "_metadata_is_cdc";
  public static final String METADATA_DLQ_RECONSUMED = "_metadata_dlq_reconsumed";
  public static final String ORIGINAL_DOCUMENT = "_original_document";

  public enum ErrorType {
    RETRYABLE,
    PERMANENT
  }

  public enum FailureStage {
    UDF,
    VALIDATE,
    WRITE
  }

  public enum OperationType {
    INSERT,
    UPDATE,
    REPLACE,
    DELETE,
    DROP,
    RENAME,
    BACKFILL;

    public boolean isDelete() {
      return this == DELETE;
    }

    public boolean isUpsert() {
      return this == INSERT || this == UPDATE || this == REPLACE || this == BACKFILL;
    }
  }

  private final Document document;
  private final String originalDocument;
  private final Integer retryCount;
  private final String errorMessage;
  private final ErrorType errorType;
  private final String sourceCollection;
  private final String targetCollection;
  private final FailureStage failureStage;
  private final OperationType operationType;
  private final TimestampSortKey timestampSortKey;
  private final String documentKey;
  private final boolean isDlqReconsumed;

  public DocumentWithMetadata(
      Document document,
      String originalDocument,
      Integer retryCount,
      String errorMessage,
      ErrorType errorType,
      String sourceCollection,
      String targetCollection,
      FailureStage failureStage,
      OperationType operationType,
      TimestampSortKey timestampSortKey,
      String documentKey,
      boolean isDlqReconsumed) {
    this.document = document;
    this.originalDocument = originalDocument;
    this.retryCount = retryCount != null ? retryCount : 0;
    this.errorMessage = errorMessage;
    this.errorType = errorType;
    this.sourceCollection = sourceCollection;
    this.targetCollection = targetCollection;
    this.failureStage = failureStage;
    this.operationType = operationType != null ? operationType : OperationType.BACKFILL;
    this.timestampSortKey = timestampSortKey;
    this.documentKey = documentKey;
    this.isDlqReconsumed = isDlqReconsumed;
  }

  public DocumentWithMetadata(
      Document document,
      String originalDocument,
      Integer retryCount,
      String errorMessage,
      ErrorType errorType,
      String sourceCollection,
      String targetCollection,
      FailureStage failureStage) {
    this(
        document,
        originalDocument,
        retryCount,
        errorMessage,
        errorType,
        sourceCollection,
        targetCollection,
        failureStage,
        OperationType.BACKFILL,
        null,
        null,
        false);
  }

  /** Returns the current BSON document. */
  public Document getDocument() {
    return document;
  }

  /** Returns the ID of the document. */
  public Object getId() {
    if (document != null && document.containsKey("_id")) {
      return document.get("_id");
    }
    if (documentKey != null) {
      try {
        Document keyDoc = Document.parse(documentKey);
        if (keyDoc.containsKey("_id")) {
          return keyDoc.get("_id");
        }
      } catch (Exception ignored) {
        return documentKey;
      }
    }
    return null;
  }

  /**
   * Returns a unique deduplication key scoped by collection and document identifier.
   */
  public String getDedupKey() {
    String col;
    if (targetCollection != null && !targetCollection.isEmpty()) {
      col = targetCollection;
    } else if (sourceCollection != null && !sourceCollection.isEmpty()) {
      col = sourceCollection;
    } else {
      col = "default";
    }
    if (documentKey != null && !documentKey.isEmpty()) {
      return col + "#" + documentKey;
    }
    if (document != null && document.containsKey("_id")) {
      try {
        return col + "#" + new Document("_id", document.get("_id")).toJson(CANONICAL_JSON_SETTINGS);
      } catch (Exception ignored) {
        return col + "#" + document.get("_id");
      }
    }
    Object id = getId();
    if (id != null) {
      return col + "#" + id;
    }
    return col + "#" + System.identityHashCode(this);
  }

  /** Returns the original document string. */
  public String getOriginalDocument() {
    if (originalDocument != null) {
      return originalDocument;
    }
    if (document != null) {
      return document.toJson(CANONICAL_JSON_SETTINGS);
    }
    return null;
  }

  /** Returns the retry count associated with this document in DLQ. */
  public Integer getRetryCount() {
    return retryCount;
  }

  /** Returns the error message that caused this document to be sent to DLQ. */
  public String getErrorMessage() {
    return errorMessage;
  }

  /** Returns the error type (e.g., RETRYABLE, PERMANENT). */
  public ErrorType getErrorType() {
    return errorType;
  }

  /** Returns the source collection name. */
  public String getSourceCollection() {
    return sourceCollection;
  }

  /** Returns the target collection name. */
  public String getTargetCollection() {
    return targetCollection;
  }

  /** Returns the failure stage. */
  public FailureStage getFailureStage() {
    return failureStage;
  }

  /** Returns the CDC operation type (e.g., INSERT, UPDATE, REPLACE, DELETE, BACKFILL). */
  public OperationType getOperationType() {
    return operationType;
  }

  /** Returns the ordering timestamp sort key. */
  public TimestampSortKey getTimestampSortKey() {
    return timestampSortKey;
  }

  /** Returns the document key string for identification/deletions. */
  public String getDocumentKey() {
    return documentKey;
  }

  /** Returns whether this event is a reconsumed DLQ retry. */
  public boolean isDlqReconsumed() {
    return isDlqReconsumed;
  }

  public DocumentWithMetadata withTimestampSortKey(TimestampSortKey key) {
    return new DocumentWithMetadata(
        document,
        originalDocument,
        retryCount,
        errorMessage,
        errorType,
        sourceCollection,
        targetCollection,
        failureStage,
        operationType,
        key,
        documentKey,
        isDlqReconsumed);
  }

  public DocumentWithMetadata withOperationType(OperationType op) {
    return new DocumentWithMetadata(
        document,
        originalDocument,
        retryCount,
        errorMessage,
        errorType,
        sourceCollection,
        targetCollection,
        failureStage,
        op,
        timestampSortKey,
        documentKey,
        isDlqReconsumed);
  }

  public DocumentWithMetadata withDocumentKey(String docKey) {
    return new DocumentWithMetadata(
        document,
        originalDocument,
        retryCount,
        errorMessage,
        errorType,
        sourceCollection,
        targetCollection,
        failureStage,
        operationType,
        timestampSortKey,
        docKey,
        isDlqReconsumed);
  }

  public DocumentWithMetadata withDlqReconsumed(boolean dlqReconsumed) {
    return new DocumentWithMetadata(
        document,
        originalDocument,
        retryCount,
        errorMessage,
        errorType,
        sourceCollection,
        targetCollection,
        failureStage,
        operationType,
        timestampSortKey,
        documentKey,
        dlqReconsumed);
  }

  public DocumentWithMetadata withDocument(Document newDoc) {
    return new DocumentWithMetadata(
        newDoc,
        getOriginalDocument(),
        retryCount,
        errorMessage,
        errorType,
        sourceCollection,
        targetCollection,
        failureStage,
        operationType,
        timestampSortKey,
        documentKey,
        isDlqReconsumed);
  }

  public DocumentWithMetadata withFailure(
      String errorMsg, ErrorType errType, FailureStage stage) {
    return new DocumentWithMetadata(
        document,
        originalDocument,
        retryCount,
        errorMsg,
        errType,
        sourceCollection,
        targetCollection,
        stage,
        operationType,
        timestampSortKey,
        documentKey,
        isDlqReconsumed);
  }

  public DocumentWithMetadata withFailure(
      String errorMsg, ErrorType errType, FailureStage stage, Integer newRetryCount) {
    return new DocumentWithMetadata(
        document,
        originalDocument,
        newRetryCount,
        errorMsg,
        errType,
        sourceCollection,
        targetCollection,
        stage,
        operationType,
        timestampSortKey,
        documentKey,
        true);
  }

  public static DocumentWithMetadata of(
      Document document,
      String originalDocument,
      Integer retryCount,
      String errorMessage,
      ErrorType errorType) {
    return new DocumentWithMetadata(
        document, originalDocument, retryCount, errorMessage, errorType, null, null, null);
  }

  public static DocumentWithMetadata of(
      Document document,
      String originalDocument,
      Integer retryCount,
      String errorMessage,
      ErrorType errorType,
      String sourceCollection,
      String targetCollection) {
    return new DocumentWithMetadata(
        document,
        originalDocument,
        retryCount,
        errorMessage,
        errorType,
        sourceCollection,
        targetCollection,
        null);
  }

  public static DocumentWithMetadata of(Document document, String originalDocument) {
    return new DocumentWithMetadata(document, originalDocument, 0, null, null, null, null, null);
  }

  public static DocumentWithMetadata of(Document document) {
    return new DocumentWithMetadata(document, null, 0, null, null, null, null, null);
  }

  public static DocumentWithMetadata of(
      Document document, String sourceCollection, String targetCollection) {
    return new DocumentWithMetadata(
        document,
        null,
        0,
        null,
        null,
        sourceCollection,
        targetCollection,
        null);
  }

  public static DocumentWithMetadata of(
      Document document,
      String originalDocument,
      Integer retryCount,
      String errorMessage,
      ErrorType errorType,
      String sourceCollection,
      String targetCollection,
      FailureStage failureStage) {
    return new DocumentWithMetadata(
        document,
        originalDocument,
        retryCount,
        errorMessage,
        errorType,
        sourceCollection,
        targetCollection,
        failureStage);
  }

  public static DocumentWithMetadata cdcEvent(
      Document document,
      String originalDocument,
      String sourceCollection,
      String targetCollection,
      OperationType operationType,
      TimestampSortKey timestampSortKey,
      String documentKey) {
    return new DocumentWithMetadata(
        document,
        originalDocument,
        0,
        null,
        null,
        sourceCollection,
        targetCollection,
        null,
        operationType,
        timestampSortKey,
        documentKey,
        false);
  }

  public static DocumentWithMetadata backfillEvent(
      Document document,
      String sourceCollection,
      String targetCollection,
      TimestampSortKey timestampSortKey) {
    String docKey = null;
    if (document != null && document.containsKey("_id")) {
      docKey = new Document("_id", document.get("_id")).toJson(CANONICAL_JSON_SETTINGS);
    }
    return new DocumentWithMetadata(
        document,
        null,
        0,
        null,
        null,
        sourceCollection,
        targetCollection,
        null,
        OperationType.BACKFILL,
        timestampSortKey,
        docKey,
        false);
  }

  /**
   * Serializes the event for DLQ.
   *
   * @param errorMessage the error message
   * @param errorType the error type (PERMANENT or RETRYABLE)
   * @param newRetryCount the new retry count
   * @return the JSON string ready for DLQ
   */
  public String toDlqJson(String errorMessage, ErrorType errorType, Integer newRetryCount) {
    try {
      ObjectNode dlqNode = MAPPER.createObjectNode();

      if (originalDocument != null && !originalDocument.isEmpty()) {
        dlqNode.set(ORIGINAL_DOCUMENT, MAPPER.readTree(originalDocument));
      } else if (document != null) {
        dlqNode.set(ORIGINAL_DOCUMENT, MAPPER.readTree(document.toJson(CANONICAL_JSON_SETTINGS)));
      } else {
        dlqNode.putNull(ORIGINAL_DOCUMENT);
      }

      dlqNode.put(METADATA_ERROR_MESSAGE, errorMessage);
      dlqNode.put(METADATA_ERROR_TYPE, errorType != null ? errorType.name() : null);
      dlqNode.put(METADATA_RETRY_COUNT, newRetryCount);
      dlqNode.put(METADATA_SOURCE_COLLECTION, sourceCollection);
      dlqNode.put(METADATA_TARGET_COLLECTION, targetCollection);
      dlqNode.put(METADATA_FAILURE_STAGE, failureStage != null ? failureStage.name() : null);
      dlqNode.put(METADATA_OPERATION_TYPE, operationType != null ? operationType.name() : null);
      dlqNode.put(METADATA_DOCUMENT_KEY, documentKey);
      dlqNode.put(METADATA_DLQ_RECONSUMED, true);

      if (timestampSortKey != null) {
        dlqNode.put(METADATA_TIMESTAMP_SECONDS, timestampSortKey.getSeconds());
        dlqNode.put(METADATA_TIMESTAMP_SUB_SECONDS, timestampSortKey.getSubSeconds());
        dlqNode.put(METADATA_IS_CDC, timestampSortKey.isCdc());
      }

      return dlqNode.toString();
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize DLQ message", e);
    }
  }

  private static String getOrDefault(JsonNode node, String fieldName, String defaultValue) {
    return node.has(fieldName) && !node.get(fieldName).isNull()
        ? node.get(fieldName).asText()
        : defaultValue;
  }

  private static int getIntOrDefault(JsonNode node, String fieldName, int defaultValue) {
    return node.has(fieldName) && !node.get(fieldName).isNull()
        ? node.get(fieldName).asInt()
        : defaultValue;
  }

  private static long getLongOrDefault(JsonNode node, String fieldName, long defaultValue) {
    return node.has(fieldName) && !node.get(fieldName).isNull()
        ? node.get(fieldName).asLong()
        : defaultValue;
  }

  private static boolean getBooleanOrDefault(
      JsonNode node, String fieldName, boolean defaultValue) {
    return node.has(fieldName) && !node.get(fieldName).isNull()
        ? node.get(fieldName).asBoolean()
        : defaultValue;
  }

  public static DocumentWithMetadata fromDlqJson(String jsonStr) {
    try {
      JsonNode jsonNode = MAPPER.readTree(jsonStr);
      JsonNode dataNode = jsonNode.get(ORIGINAL_DOCUMENT);
      if (dataNode == null) {
        dataNode = jsonNode.get("_metadata_original_document");
      }
      if (dataNode == null) {
        // Fallback to legacy "data" key for backwards compatibility
        dataNode = jsonNode.get("data");
      }
      String documentKey = getOrDefault(jsonNode, METADATA_DOCUMENT_KEY, null);

      if ((dataNode == null || dataNode.isNull()) && documentKey == null) {
        throw new IllegalArgumentException(
            "Invalid DLQ message: missing '" + ORIGINAL_DOCUMENT + "' or 'data' field");
      }

      Document doc = null;
      String originalDocument = null;
      if (dataNode != null && !dataNode.isNull()) {
        doc = Document.parse(dataNode.toString());
        originalDocument = dataNode.toString();
      }

      Integer retryCount = getIntOrDefault(jsonNode, METADATA_RETRY_COUNT, 0);
      String errorMsg = getOrDefault(jsonNode, METADATA_ERROR_MESSAGE, null);
      String errorTypeStr = getOrDefault(jsonNode, METADATA_ERROR_TYPE, null);
      ErrorType errorType = errorTypeStr != null ? ErrorType.valueOf(errorTypeStr) : null;
      String sourceCollection = getOrDefault(jsonNode, METADATA_SOURCE_COLLECTION, null);
      String targetCollection = getOrDefault(jsonNode, METADATA_TARGET_COLLECTION, null);
      String failureStageStr = getOrDefault(jsonNode, METADATA_FAILURE_STAGE, null);
      FailureStage failureStage =
          failureStageStr != null ? FailureStage.valueOf(failureStageStr) : null;

      String operationTypeStr = getOrDefault(jsonNode, METADATA_OPERATION_TYPE, null);
      OperationType operationType =
          operationTypeStr != null
              ? OperationType.valueOf(operationTypeStr)
              : OperationType.BACKFILL;
      boolean isDlqReconsumed = getBooleanOrDefault(jsonNode, METADATA_DLQ_RECONSUMED, true);

      TimestampSortKey timestampSortKey = null;
      if (jsonNode.has(METADATA_TIMESTAMP_SECONDS)
          && !jsonNode.get(METADATA_TIMESTAMP_SECONDS).isNull()) {
        long sec = getLongOrDefault(jsonNode, METADATA_TIMESTAMP_SECONDS, 0L);
        long subSec = getLongOrDefault(jsonNode, METADATA_TIMESTAMP_SUB_SECONDS, 0L);
        boolean isCdc = getBooleanOrDefault(jsonNode, METADATA_IS_CDC, false);
        timestampSortKey = TimestampSortKey.of(sec, subSec, isCdc);
      }

      return new DocumentWithMetadata(
          doc,
          originalDocument,
          retryCount,
          errorMsg,
          errorType,
          sourceCollection,
          targetCollection,
          failureStage,
          operationType,
          timestampSortKey,
          documentKey,
          isDlqReconsumed);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse DLQ message", e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DocumentWithMetadata that)) {
      return false;
    }
    return isDlqReconsumed == that.isDlqReconsumed
        && Objects.equals(document, that.document)
        && Objects.equals(originalDocument, that.originalDocument)
        && Objects.equals(retryCount, that.retryCount)
        && Objects.equals(errorMessage, that.errorMessage)
        && errorType == that.errorType
        && Objects.equals(sourceCollection, that.sourceCollection)
        && Objects.equals(targetCollection, that.targetCollection)
        && failureStage == that.failureStage
        && operationType == that.operationType
        && Objects.equals(timestampSortKey, that.timestampSortKey)
        && Objects.equals(documentKey, that.documentKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        document,
        originalDocument,
        retryCount,
        errorMessage,
        errorType,
        sourceCollection,
        targetCollection,
        failureStage,
        operationType,
        timestampSortKey,
        documentKey,
        isDlqReconsumed);
  }
}
