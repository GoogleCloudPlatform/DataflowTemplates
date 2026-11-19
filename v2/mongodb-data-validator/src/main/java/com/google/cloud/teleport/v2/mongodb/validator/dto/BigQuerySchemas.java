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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;

/** BigQuery table schemas for MongoDB-to-Firestore data validation reporting. */
public final class BigQuerySchemas {

  public static final String MISMATCHED_RECORDS_TABLE = "MismatchedRecords";
  public static final String COLLECTION_VALIDATION_STATS_TABLE = "CollectionValidationStats";
  public static final String VALIDATION_SUMMARY_TABLE = "ValidationSummary";

  public static final TableSchema MISMATCHED_RECORDS_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  requiredField(MismatchedDocument.RUN_ID_COLUMN, "STRING"),
                  requiredField(MismatchedDocument.SOURCE_DB_COLUMN, "STRING"),
                  requiredField(MismatchedDocument.TARGET_DB_COLUMN, "STRING"),
                  requiredField(MismatchedDocument.SOURCE_COLLECTION_COLUMN, "STRING"),
                  requiredField(MismatchedDocument.TARGET_COLLECTION_COLUMN, "STRING"),
                  requiredField(MismatchedDocument.DOCUMENT_ID_COLUMN, "STRING"),
                  requiredField(MismatchedDocument.MISMATCH_TYPE_COLUMN, "STRING"),
                  nullableField(MismatchedDocument.DIFF_DETAILS_COLUMN, "STRING"),
                  nullableField(MismatchedDocument.SLICE_ID_COLUMN, "STRING"),
                  requiredField(MismatchedDocument.DETECTED_AT_COLUMN, "TIMESTAMP")));

  public static final TableSchema COLLECTION_VALIDATION_STATS_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  requiredField(CollectionValidationStats.RUN_ID_COLUMN, "STRING"),
                  requiredField(CollectionValidationStats.SOURCE_DB_COLUMN, "STRING"),
                  requiredField(CollectionValidationStats.TARGET_DB_COLUMN, "STRING"),
                  requiredField(CollectionValidationStats.SOURCE_COLLECTION_COLUMN, "STRING"),
                  requiredField(CollectionValidationStats.TARGET_COLLECTION_COLUMN, "STRING"),
                  requiredField(CollectionValidationStats.STATUS_COLUMN, "STRING"),
                  requiredField(CollectionValidationStats.SOURCE_DOC_COUNT_COLUMN, "INTEGER"),
                  requiredField(CollectionValidationStats.TARGET_DOC_COUNT_COLUMN, "INTEGER"),
                  requiredField(CollectionValidationStats.MATCHED_DOC_COUNT_COLUMN, "INTEGER"),
                  requiredField(CollectionValidationStats.MISMATCH_DOC_COUNT_COLUMN, "INTEGER"),
                  requiredField(CollectionValidationStats.RESOLVED_ON_RECHECK_COLUMN, "INTEGER"),
                  requiredField(CollectionValidationStats.INDEX_MISMATCH_COUNT_COLUMN, "INTEGER"),
                  requiredField(CollectionValidationStats.START_TIMESTAMP_COLUMN, "TIMESTAMP"),
                  requiredField(CollectionValidationStats.END_TIMESTAMP_COLUMN, "TIMESTAMP")));

  public static final TableSchema VALIDATION_SUMMARY_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  requiredField(ValidationSummary.RUN_ID_COLUMN, "STRING"),
                  requiredField(ValidationSummary.SOURCE_DB_COLUMN, "STRING"),
                  requiredField(ValidationSummary.TARGET_DB_COLUMN, "STRING"),
                  requiredField(ValidationSummary.STATUS_COLUMN, "STRING"),
                  requiredField(ValidationSummary.TOTAL_COLLECTIONS_COLUMN, "INTEGER"),
                  requiredField(ValidationSummary.MATCHED_COLLECTIONS_COLUMN, "INTEGER"),
                  requiredField(ValidationSummary.MISMATCHED_COLLECTIONS_COLUMN, "INTEGER"),
                  requiredField(ValidationSummary.TOTAL_SOURCE_DOCS_COLUMN, "INTEGER"),
                  requiredField(ValidationSummary.TOTAL_TARGET_DOCS_COLUMN, "INTEGER"),
                  requiredField(ValidationSummary.TOTAL_MATCHED_DOCS_COLUMN, "INTEGER"),
                  requiredField(ValidationSummary.TOTAL_MISMATCHED_DOCS_COLUMN, "INTEGER"),
                  requiredField(ValidationSummary.TOTAL_RESOLVED_RECHECK_COLUMN, "INTEGER"),
                  requiredField(ValidationSummary.TOTAL_INDEX_MISMATCHES_COLUMN, "INTEGER"),
                  requiredField(ValidationSummary.START_TIMESTAMP_COLUMN, "TIMESTAMP"),
                  requiredField(ValidationSummary.END_TIMESTAMP_COLUMN, "TIMESTAMP")));

  private BigQuerySchemas() {}

  private static TableFieldSchema requiredField(String name, String type) {
    return new TableFieldSchema().setName(name).setType(type).setMode("REQUIRED");
  }

  private static TableFieldSchema nullableField(String name, String type) {
    return new TableFieldSchema().setName(name).setType(type).setMode("NULLABLE");
  }
}
