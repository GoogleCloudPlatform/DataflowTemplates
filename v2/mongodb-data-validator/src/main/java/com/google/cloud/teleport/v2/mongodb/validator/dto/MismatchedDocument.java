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
package com.google.cloud.teleport.v2.mongodb.validator.dto;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.JsonObject;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;

/** Represents a single document, index, or collection discrepancy detected during validation. */
public final class MismatchedDocument implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String RUN_ID_COLUMN = "run_id";
  public static final String SOURCE_DB_COLUMN = "source_database";
  public static final String TARGET_DB_COLUMN = "target_database";
  public static final String SOURCE_COLLECTION_COLUMN = "source_collection";
  public static final String TARGET_COLLECTION_COLUMN = "target_collection";
  public static final String DOCUMENT_ID_COLUMN = "document_id_ejson";
  public static final String MISMATCH_TYPE_COLUMN = "mismatch_type";
  public static final String DIFF_DETAILS_COLUMN = "diff_details";
  public static final String SLICE_ID_COLUMN = "slice_id";
  public static final String DETECTED_AT_COLUMN = "detected_at";

  private final String runId;
  private final String sourceDatabase;
  private final String targetDatabase;
  private final String sourceCollection;
  private final String targetCollection;
  private final String documentIdExtendedJson;
  @Nullable private final byte[] rawIdBsonBytes;
  private final MismatchType mismatchType;
  private final String diffDetails;
  private final String sliceId;
  private final String detectedAtIso;
  private final int estimatedBytes;

  public MismatchedDocument(
      String runId,
      String sourceDatabase,
      String targetDatabase,
      String sourceCollection,
      String targetCollection,
      String documentIdExtendedJson,
      @Nullable byte[] rawIdBsonBytes,
      MismatchType mismatchType,
      String diffDetails,
      String sliceId,
      String detectedAtIso,
      int estimatedBytes) {
    this.runId = Objects.requireNonNull(runId, "runId");
    this.sourceDatabase = Objects.requireNonNull(sourceDatabase, "sourceDatabase");
    this.targetDatabase = Objects.requireNonNull(targetDatabase, "targetDatabase");
    this.sourceCollection = Objects.requireNonNull(sourceCollection, "sourceCollection");
    this.targetCollection = Objects.requireNonNull(targetCollection, "targetCollection");
    this.documentIdExtendedJson =
        Objects.requireNonNull(documentIdExtendedJson, "documentIdExtendedJson");
    this.rawIdBsonBytes = rawIdBsonBytes == null ? null : rawIdBsonBytes.clone();
    this.mismatchType = Objects.requireNonNull(mismatchType, "mismatchType");
    this.diffDetails = Objects.requireNonNull(diffDetails, "diffDetails");
    this.sliceId = Objects.requireNonNull(sliceId, "sliceId");
    this.detectedAtIso = Objects.requireNonNull(detectedAtIso, "detectedAtIso");
    this.estimatedBytes = Math.max(64, estimatedBytes);
  }

  public String getRunId() {
    return runId;
  }

  public String getSourceDatabase() {
    return sourceDatabase;
  }

  public String getTargetDatabase() {
    return targetDatabase;
  }

  public String getSourceCollection() {
    return sourceCollection;
  }

  public String getTargetCollection() {
    return targetCollection;
  }

  public String getDocumentIdExtendedJson() {
    return documentIdExtendedJson;
  }

  @Nullable
  public byte[] getRawIdBsonBytes() {
    return rawIdBsonBytes == null ? null : rawIdBsonBytes.clone();
  }

  public MismatchType getMismatchType() {
    return mismatchType;
  }

  public String getDiffDetails() {
    return diffDetails;
  }

  public String getSliceId() {
    return sliceId;
  }

  public String getDetectedAtIso() {
    return detectedAtIso;
  }

  public int getEstimatedBytes() {
    return estimatedBytes;
  }

  /** Serializes this discrepancy as a single JSON line suitable for repair tooling. */
  public String toJsonLine() {
    JsonObject obj = new JsonObject();
    obj.addProperty(RUN_ID_COLUMN, runId);
    obj.addProperty(SOURCE_DB_COLUMN, sourceDatabase);
    obj.addProperty(TARGET_DB_COLUMN, targetDatabase);
    obj.addProperty(SOURCE_COLLECTION_COLUMN, sourceCollection);
    obj.addProperty(TARGET_COLLECTION_COLUMN, targetCollection);
    obj.addProperty(DOCUMENT_ID_COLUMN, documentIdExtendedJson);
    obj.addProperty(MISMATCH_TYPE_COLUMN, mismatchType.name());
    obj.addProperty(DIFF_DETAILS_COLUMN, diffDetails);
    obj.addProperty(SLICE_ID_COLUMN, sliceId);
    obj.addProperty(DETECTED_AT_COLUMN, detectedAtIso);
    return obj.toString();
  }

  /** Converts this discrepancy into a BigQuery {@link TableRow}. */
  public TableRow toTableRow() {
    return new TableRow()
        .set(RUN_ID_COLUMN, runId)
        .set(SOURCE_DB_COLUMN, sourceDatabase)
        .set(TARGET_DB_COLUMN, targetDatabase)
        .set(SOURCE_COLLECTION_COLUMN, sourceCollection)
        .set(TARGET_COLLECTION_COLUMN, targetCollection)
        .set(DOCUMENT_ID_COLUMN, documentIdExtendedJson)
        .set(MISMATCH_TYPE_COLUMN, mismatchType.name())
        .set(DIFF_DETAILS_COLUMN, diffDetails)
        .set(SLICE_ID_COLUMN, sliceId)
        .set(DETECTED_AT_COLUMN, detectedAtIso);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MismatchedDocument)) {
      return false;
    }
    MismatchedDocument that = (MismatchedDocument) o;
    return estimatedBytes == that.estimatedBytes
        && Objects.equals(runId, that.runId)
        && Objects.equals(sourceDatabase, that.sourceDatabase)
        && Objects.equals(targetDatabase, that.targetDatabase)
        && Objects.equals(sourceCollection, that.sourceCollection)
        && Objects.equals(targetCollection, that.targetCollection)
        && Objects.equals(documentIdExtendedJson, that.documentIdExtendedJson)
        && Arrays.equals(rawIdBsonBytes, that.rawIdBsonBytes)
        && mismatchType == that.mismatchType
        && Objects.equals(diffDetails, that.diffDetails)
        && Objects.equals(sliceId, that.sliceId)
        && Objects.equals(detectedAtIso, that.detectedAtIso);
  }

  @Override
  public int hashCode() {
    int result =
        Objects.hash(
            runId,
            sourceDatabase,
            targetDatabase,
            sourceCollection,
            targetCollection,
            documentIdExtendedJson,
            mismatchType,
            diffDetails,
            sliceId,
            detectedAtIso,
            estimatedBytes);
    return 31 * result + Arrays.hashCode(rawIdBsonBytes);
  }
}
