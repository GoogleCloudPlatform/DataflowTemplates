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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import java.io.Serializable;
import java.util.Objects;

/** Run-wide validation summary across all collections in the database pair. */
public final class ValidationSummary implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String RUN_ID_COLUMN = "run_id";
  public static final String SOURCE_DB_COLUMN = "source_database";
  public static final String TARGET_DB_COLUMN = "target_database";
  public static final String STATUS_COLUMN = "status";
  public static final String TOTAL_COLLECTIONS_COLUMN = "total_collections";
  public static final String MATCHED_COLLECTIONS_COLUMN = "matched_collections";
  public static final String MISMATCHED_COLLECTIONS_COLUMN = "mismatched_collections";
  public static final String TOTAL_SOURCE_DOCS_COLUMN = "total_source_docs";
  public static final String TOTAL_TARGET_DOCS_COLUMN = "total_target_docs";
  public static final String TOTAL_MATCHED_DOCS_COLUMN = "total_matched_docs";
  public static final String TOTAL_MISMATCHED_DOCS_COLUMN = "total_mismatched_docs";
  public static final String TOTAL_RESOLVED_RECHECK_COLUMN = "total_resolved_on_recheck_docs";
  public static final String TOTAL_INDEX_MISMATCHES_COLUMN = "total_index_mismatches";
  public static final String START_TIMESTAMP_COLUMN = "start_timestamp";
  public static final String END_TIMESTAMP_COLUMN = "end_timestamp";

  private final String runId;
  private final String sourceDatabase;
  private final String targetDatabase;
  private final String status;
  private final long totalCollections;
  private final long matchedCollections;
  private final long mismatchedCollections;
  private final long totalSourceDocs;
  private final long totalTargetDocs;
  private final long totalMatchedDocs;
  private final long totalMismatchedDocs;
  private final long totalResolvedOnRecheckDocs;
  private final long totalIndexMismatches;
  private final String startTimestampIso;
  private final String endTimestampIso;

  public ValidationSummary(
      String runId,
      String sourceDatabase,
      String targetDatabase,
      String status,
      long totalCollections,
      long matchedCollections,
      long mismatchedCollections,
      long totalSourceDocs,
      long totalTargetDocs,
      long totalMatchedDocs,
      long totalMismatchedDocs,
      long totalResolvedOnRecheckDocs,
      long totalIndexMismatches,
      String startTimestampIso,
      String endTimestampIso) {
    this.runId = Objects.requireNonNull(runId, "runId");
    this.sourceDatabase = Objects.requireNonNull(sourceDatabase, "sourceDatabase");
    this.targetDatabase = Objects.requireNonNull(targetDatabase, "targetDatabase");
    this.status = Objects.requireNonNull(status, "status");
    this.totalCollections = totalCollections;
    this.matchedCollections = matchedCollections;
    this.mismatchedCollections = mismatchedCollections;
    this.totalSourceDocs = totalSourceDocs;
    this.totalTargetDocs = totalTargetDocs;
    this.totalMatchedDocs = totalMatchedDocs;
    this.totalMismatchedDocs = totalMismatchedDocs;
    this.totalResolvedOnRecheckDocs = totalResolvedOnRecheckDocs;
    this.totalIndexMismatches = totalIndexMismatches;
    this.startTimestampIso = Objects.requireNonNull(startTimestampIso, "startTimestampIso");
    this.endTimestampIso = Objects.requireNonNull(endTimestampIso, "endTimestampIso");
  }

  public String getStatus() {
    return status;
  }

  public long getTotalMismatchedDocs() {
    return totalMismatchedDocs;
  }

  public long getTotalIndexMismatches() {
    return totalIndexMismatches;
  }

  public String toPrettyJson() {
    JsonObject obj = new JsonObject();
    obj.addProperty(RUN_ID_COLUMN, runId);
    obj.addProperty(SOURCE_DB_COLUMN, sourceDatabase);
    obj.addProperty(TARGET_DB_COLUMN, targetDatabase);
    obj.addProperty(STATUS_COLUMN, status);
    obj.addProperty(TOTAL_COLLECTIONS_COLUMN, totalCollections);
    obj.addProperty(MATCHED_COLLECTIONS_COLUMN, matchedCollections);
    obj.addProperty(MISMATCHED_COLLECTIONS_COLUMN, mismatchedCollections);
    obj.addProperty(TOTAL_SOURCE_DOCS_COLUMN, totalSourceDocs);
    obj.addProperty(TOTAL_TARGET_DOCS_COLUMN, totalTargetDocs);
    obj.addProperty(TOTAL_MATCHED_DOCS_COLUMN, totalMatchedDocs);
    obj.addProperty(TOTAL_MISMATCHED_DOCS_COLUMN, totalMismatchedDocs);
    obj.addProperty(TOTAL_RESOLVED_RECHECK_COLUMN, totalResolvedOnRecheckDocs);
    obj.addProperty(TOTAL_INDEX_MISMATCHES_COLUMN, totalIndexMismatches);
    obj.addProperty(START_TIMESTAMP_COLUMN, startTimestampIso);
    obj.addProperty(END_TIMESTAMP_COLUMN, endTimestampIso);
    return new GsonBuilder().setPrettyPrinting().create().toJson(obj);
  }

  public TableRow toTableRow() {
    return new TableRow()
        .set(RUN_ID_COLUMN, runId)
        .set(SOURCE_DB_COLUMN, sourceDatabase)
        .set(TARGET_DB_COLUMN, targetDatabase)
        .set(STATUS_COLUMN, status)
        .set(TOTAL_COLLECTIONS_COLUMN, totalCollections)
        .set(MATCHED_COLLECTIONS_COLUMN, matchedCollections)
        .set(MISMATCHED_COLLECTIONS_COLUMN, mismatchedCollections)
        .set(TOTAL_SOURCE_DOCS_COLUMN, totalSourceDocs)
        .set(TOTAL_TARGET_DOCS_COLUMN, totalTargetDocs)
        .set(TOTAL_MATCHED_DOCS_COLUMN, totalMatchedDocs)
        .set(TOTAL_MISMATCHED_DOCS_COLUMN, totalMismatchedDocs)
        .set(TOTAL_RESOLVED_RECHECK_COLUMN, totalResolvedOnRecheckDocs)
        .set(TOTAL_INDEX_MISMATCHES_COLUMN, totalIndexMismatches)
        .set(START_TIMESTAMP_COLUMN, startTimestampIso)
        .set(END_TIMESTAMP_COLUMN, endTimestampIso);
  }
}
