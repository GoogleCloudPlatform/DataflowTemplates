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
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

/**
 * This class contains the raw document and metadata related to the migration. It is used to carry
 * the document and its DLQ retry context through the pipeline without polluting the original
 * document schema.
 */
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

  public enum ErrorType {
    RETRYABLE,
    PERMANENT
  }

  public enum FailureStage {
    UDF,
    VALIDATE,
    WRITE
  }

  private final Document document;
  private final String originalDocument;
  private final Integer retryCount;
  private final String errorMessage;
  private final ErrorType errorType;
  private final String sourceCollection;
  private final String targetCollection;
  private final FailureStage failureStage;

  public DocumentWithMetadata(
      Document document,
      String originalDocument,
      Integer retryCount,
      String errorMessage,
      ErrorType errorType,
      String sourceCollection,
      String targetCollection,
      FailureStage failureStage) {
    this.document = document;
    this.originalDocument = originalDocument;
    this.retryCount = retryCount;
    this.errorMessage = errorMessage;
    this.errorType = errorType;
    this.sourceCollection = sourceCollection;
    this.targetCollection = targetCollection;
    this.failureStage = failureStage;
  }

  /** Returns the current BSON document. */
  public Document getDocument() {
    return document;
  }

  /** Returns the ID of the document. */
  public Object getId() {
    if (document != null) {
      return document.get("_id");
    }
    return null;
  }

  /** Returns the original document string. */
  public String getOriginalDocument() {
    return originalDocument;
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
    return new DocumentWithMetadata(
        document, document.toJson(CANONICAL_JSON_SETTINGS), 0, null, null, null, null, null);
  }

  public static DocumentWithMetadata of(
      Document document, String sourceCollection, String targetCollection) {
    return new DocumentWithMetadata(
        document,
        document.toJson(CANONICAL_JSON_SETTINGS),
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

      dlqNode.set("data", MAPPER.readTree(originalDocument));
      dlqNode.put(METADATA_ERROR_MESSAGE, errorMessage);
      dlqNode.put(METADATA_ERROR_TYPE, errorType != null ? errorType.name() : null);
      dlqNode.put(METADATA_RETRY_COUNT, newRetryCount);
      dlqNode.put(METADATA_SOURCE_COLLECTION, sourceCollection);
      dlqNode.put(METADATA_TARGET_COLLECTION, targetCollection);
      dlqNode.put(METADATA_FAILURE_STAGE, failureStage != null ? failureStage.name() : null);

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

  public static DocumentWithMetadata fromDlqJson(String jsonStr) {
    try {
      JsonNode jsonNode = MAPPER.readTree(jsonStr);
      JsonNode dataNode = jsonNode.get("data");
      if (dataNode == null) {
        throw new IllegalArgumentException("Invalid DLQ message: missing 'data' field");
      }

      Document doc = Document.parse(dataNode.toString());
      String originalDocument = dataNode.toString();

      Integer retryCount = getIntOrDefault(jsonNode, METADATA_RETRY_COUNT, 0);
      String errorMsg = getOrDefault(jsonNode, METADATA_ERROR_MESSAGE, null);
      String errorTypeStr = getOrDefault(jsonNode, METADATA_ERROR_TYPE, null);
      ErrorType errorType = errorTypeStr != null ? ErrorType.valueOf(errorTypeStr) : null;
      String sourceCollection = getOrDefault(jsonNode, METADATA_SOURCE_COLLECTION, null);
      String targetCollection = getOrDefault(jsonNode, METADATA_TARGET_COLLECTION, null);
      String failureStageStr = getOrDefault(jsonNode, METADATA_FAILURE_STAGE, null);
      FailureStage failureStage =
          failureStageStr != null ? FailureStage.valueOf(failureStageStr) : null;

      return new DocumentWithMetadata(
          doc,
          originalDocument,
          retryCount,
          errorMsg,
          errorType,
          sourceCollection,
          targetCollection,
          failureStage);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse DLQ message", e);
    }
  }
}
