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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

/**
 * Deterministic binary coder for {@link DocumentWithMetadata}.
 *
 * <p>Provides compact, fast binary serialization without Java reflection overhead for shuffle and
 * Windmill state storage.
 */
public class DocumentWithMetadataCoder extends AtomicCoder<DocumentWithMetadata> {

  private static final DocumentWithMetadataCoder INSTANCE = new DocumentWithMetadataCoder();
  private static final NullableCoder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
  private static final BooleanCoder BOOLEAN_CODER = BooleanCoder.of();
  private static final VarIntCoder VARINT_CODER = VarIntCoder.of();
  private static final TimestampSortKeyCoder TIMESTAMP_CODER = TimestampSortKeyCoder.of();

  private static final JsonWriterSettings CANONICAL_JSON_SETTINGS =
      JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

  private DocumentWithMetadataCoder() {}

  public static DocumentWithMetadataCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(DocumentWithMetadata value, OutputStream outStream) throws IOException {
    if (value == null) {
      BOOLEAN_CODER.encode(false, outStream);
      return;
    }
    BOOLEAN_CODER.encode(true, outStream);

    String docJson =
        value.getDocument() != null ? value.getDocument().toJson(CANONICAL_JSON_SETTINGS) : null;
    STRING_CODER.encode(docJson, outStream);
    STRING_CODER.encode(value.getOriginalDocument(), outStream);
    VARINT_CODER.encode(value.getRetryCount() != null ? value.getRetryCount() : 0, outStream);
    STRING_CODER.encode(value.getErrorMessage(), outStream);
    STRING_CODER.encode(
        value.getErrorType() != null ? value.getErrorType().name() : null, outStream);
    STRING_CODER.encode(value.getSourceCollection(), outStream);
    STRING_CODER.encode(value.getTargetCollection(), outStream);
    STRING_CODER.encode(
        value.getFailureStage() != null ? value.getFailureStage().name() : null, outStream);
    STRING_CODER.encode(
        value.getOperationType() != null ? value.getOperationType().name() : null, outStream);
    STRING_CODER.encode(value.getDocumentKey(), outStream);
    BOOLEAN_CODER.encode(value.isDlqReconsumed(), outStream);
    TIMESTAMP_CODER.encode(value.getTimestampSortKey(), outStream);
  }

  @Override
  public DocumentWithMetadata decode(InputStream inStream) throws IOException {
    boolean isPresent = BOOLEAN_CODER.decode(inStream);
    if (!isPresent) {
      return null;
    }

    String docJson = STRING_CODER.decode(inStream);
    Document doc = docJson != null ? Document.parse(docJson) : null;
    String originalDoc = STRING_CODER.decode(inStream);
    int retryCount = VARINT_CODER.decode(inStream);
    String errorMessage = STRING_CODER.decode(inStream);
    String errorTypeStr = STRING_CODER.decode(inStream);
    DocumentWithMetadata.ErrorType errorType =
        errorTypeStr != null ? DocumentWithMetadata.ErrorType.valueOf(errorTypeStr) : null;
    String sourceCollection = STRING_CODER.decode(inStream);
    String targetCollection = STRING_CODER.decode(inStream);
    String failureStageStr = STRING_CODER.decode(inStream);
    DocumentWithMetadata.FailureStage failureStage =
        failureStageStr != null
            ? DocumentWithMetadata.FailureStage.valueOf(failureStageStr)
            : null;
    String opTypeStr = STRING_CODER.decode(inStream);
    DocumentWithMetadata.OperationType opType =
        opTypeStr != null
            ? DocumentWithMetadata.OperationType.valueOf(opTypeStr)
            : DocumentWithMetadata.OperationType.BACKFILL;
    String docKey = STRING_CODER.decode(inStream);
    boolean isDlq = BOOLEAN_CODER.decode(inStream);
    TimestampSortKey timestampKey = TIMESTAMP_CODER.decode(inStream);

    return new DocumentWithMetadata(
        doc,
        originalDoc,
        retryCount,
        errorMessage,
        errorType,
        sourceCollection,
        targetCollection,
        failureStage,
        opType,
        timestampKey,
        docKey,
        isDlq);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    STRING_CODER.verifyDeterministic();
    BOOLEAN_CODER.verifyDeterministic();
    VARINT_CODER.verifyDeterministic();
    TIMESTAMP_CODER.verifyDeterministic();
  }
}
