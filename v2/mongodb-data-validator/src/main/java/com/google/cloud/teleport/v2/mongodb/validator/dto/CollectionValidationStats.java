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
import java.util.Objects;

/** Aggregated validation statistics for a single (sourceCollection, targetCollection) pair. */
public final class CollectionValidationStats implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String RUN_ID_COLUMN = "run_id";
  public static final String SOURCE_DB_COLUMN = "source_database";
  public static final String TARGET_DB_COLUMN = "target_database";
  public static final String SOURCE_COLLECTION_COLUMN = "source_collection";
  public static final String TARGET_COLLECTION_COLUMN = "target_collection";
  public static final String STATUS_COLUMN = "status";
  public static final String SOURCE_DOC_COUNT_COLUMN = "source_doc_count";
  public static final String TARGET_DOC_COUNT_COLUMN = "target_doc_count";
  public static final String MATCHED_DOC_COUNT_COLUMN = "matched_doc_count";
  public static final String MISMATCH_DOC_COUNT_COLUMN = "mismatched_doc_count";
  public static final String RESOLVED_ON_RECHECK_COLUMN = "resolved_on_recheck_count";
  public static final String INDEX_MISMATCH_COUNT_COLUMN = "index_mismatch_count";
  public static final String START_TIMESTAMP_COLUMN = "start_timestamp";
  public static final String END_TIMESTAMP_COLUMN = "end_timestamp";

  public static final String STATUS_MATCH = "MATCH";
  public static final String STATUS_MISMATCH = "MISMATCH";

  private final String runId;
  private final String sourceDatabase;
  private final String targetDatabase;
  private final String sourceCollection;
  private final String targetCollection;
  private final String status;
  private final long sourceDocCount;
  private final long targetDocCount;
  private final long matchedDocCount;
  private final long mismatchedDocCount;
  private final long resolvedOnRecheckCount;
  private final long indexMismatchCount;
  private final String startTimestampIso;
  private final String endTimestampIso;

  public CollectionValidationStats(
      String runId,
      String sourceDatabase,
      String targetDatabase,
      String sourceCollection,
      String targetCollection,
      String status,
      long sourceDocCount,
      long targetDocCount,
      long matchedDocCount,
      long mismatchedDocCount,
      long resolvedOnRecheckCount,
      long indexMismatchCount,
      String startTimestampIso,
      String endTimestampIso) {
    this.runId = Objects.requireNonNull(runId, "runId");
    this.sourceDatabase = Objects.requireNonNull(sourceDatabase, "sourceDatabase");
    this.targetDatabase = Objects.requireNonNull(targetDatabase, "targetDatabase");
    this.sourceCollection = Objects.requireNonNull(sourceCollection, "sourceCollection");
    this.targetCollection = Objects.requireNonNull(targetCollection, "targetCollection");
    this.status = Objects.requireNonNull(status, "status");
    this.sourceDocCount = sourceDocCount;
    this.targetDocCount = targetDocCount;
    this.matchedDocCount = matchedDocCount;
    this.mismatchedDocCount = mismatchedDocCount;
    this.resolvedOnRecheckCount = resolvedOnRecheckCount;
    this.indexMismatchCount = indexMismatchCount;
    this.startTimestampIso = Objects.requireNonNull(startTimestampIso, "startTimestampIso");
    this.endTimestampIso = Objects.requireNonNull(endTimestampIso, "endTimestampIso");
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

  public String getStatus() {
    return status;
  }

  public long getSourceDocCount() {
    return sourceDocCount;
  }

  public long getTargetDocCount() {
    return targetDocCount;
  }

  public long getMatchedDocCount() {
    return matchedDocCount;
  }

  public long getMismatchedDocCount() {
    return mismatchedDocCount;
  }

  public long getResolvedOnRecheckCount() {
    return resolvedOnRecheckCount;
  }

  public long getIndexMismatchCount() {
    return indexMismatchCount;
  }

  public String getStartTimestampIso() {
    return startTimestampIso;
  }

  public String getEndTimestampIso() {
    return endTimestampIso;
  }

  public String toJsonLine() {
    JsonObject obj = new JsonObject();
    obj.addProperty(RUN_ID_COLUMN, runId);
    obj.addProperty(SOURCE_DB_COLUMN, sourceDatabase);
    obj.addProperty(TARGET_DB_COLUMN, targetDatabase);
    obj.addProperty(SOURCE_COLLECTION_COLUMN, sourceCollection);
    obj.addProperty(TARGET_COLLECTION_COLUMN, targetCollection);
    obj.addProperty(STATUS_COLUMN, status);
    obj.addProperty(SOURCE_DOC_COUNT_COLUMN, sourceDocCount);
    obj.addProperty(TARGET_DOC_COUNT_COLUMN, targetDocCount);
    obj.addProperty(MATCHED_DOC_COUNT_COLUMN, matchedDocCount);
    obj.addProperty(MISMATCH_DOC_COUNT_COLUMN, mismatchedDocCount);
    obj.addProperty(RESOLVED_ON_RECHECK_COLUMN, resolvedOnRecheckCount);
    obj.addProperty(INDEX_MISMATCH_COUNT_COLUMN, indexMismatchCount);
    obj.addProperty(START_TIMESTAMP_COLUMN, startTimestampIso);
    obj.addProperty(END_TIMESTAMP_COLUMN, endTimestampIso);
    return obj.toString();
  }

  public TableRow toTableRow() {
    return new TableRow()
        .set(RUN_ID_COLUMN, runId)
        .set(SOURCE_DB_COLUMN, sourceDatabase)
        .set(TARGET_DB_COLUMN, targetDatabase)
        .set(SOURCE_COLLECTION_COLUMN, sourceCollection)
        .set(TARGET_COLLECTION_COLUMN, targetCollection)
        .set(STATUS_COLUMN, status)
        .set(SOURCE_DOC_COUNT_COLUMN, sourceDocCount)
        .set(TARGET_DOC_COUNT_COLUMN, targetDocCount)
        .set(MATCHED_DOC_COUNT_COLUMN, matchedDocCount)
        .set(MISMATCH_DOC_COUNT_COLUMN, mismatchedDocCount)
        .set(RESOLVED_ON_RECHECK_COLUMN, resolvedOnRecheckCount)
        .set(INDEX_MISMATCH_COUNT_COLUMN, indexMismatchCount)
        .set(START_TIMESTAMP_COLUMN, startTimestampIso)
        .set(END_TIMESTAMP_COLUMN, endTimestampIso);
  }
}
