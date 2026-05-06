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

import java.io.Serializable;
import org.bson.Document;

/**
 * This class contains the raw document and metadata related to the migration. It is used to carry
 * the document and its DLQ retry context through the pipeline without polluting the original
 * document schema.
 */
public class DocumentWithMetadata implements Serializable {

  private final Document document;
  private final Integer retryCount;
  private final String errorMessage;
  private final String errorType;

  public DocumentWithMetadata(
      Document document, Integer retryCount, String errorMessage, String errorType) {
    this.document = document;
    this.retryCount = retryCount;
    this.errorMessage = errorMessage;
    this.errorType = errorType;
  }

  /** Returns the raw BSON document read from the source. */
  public Document getDocument() {
    return document;
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
  public String getErrorType() {
    return errorType;
  }

  public static DocumentWithMetadata of(
      Document document, Integer retryCount, String errorMessage, String errorType) {
    return new DocumentWithMetadata(document, retryCount, errorMessage, errorType);
  }

  public static DocumentWithMetadata of(Document document) {
    return new DocumentWithMetadata(document, 0, null, null);
  }
}
