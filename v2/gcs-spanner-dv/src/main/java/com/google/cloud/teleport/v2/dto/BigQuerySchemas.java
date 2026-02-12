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
package com.google.cloud.teleport.v2.dto;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.Lists;

/** BigQuery schemas for the GCS to Spanner Data Validation pipeline. */
public final class BigQuerySchemas {

  private BigQuerySchemas() {}

  public static final TableSchema MISMATCHED_RECORDS_SCHEMA =
      new TableSchema()
          .setFields(
              Lists.newArrayList(
                  new TableFieldSchema()
                      .setName(MismatchedRecord.RUN_ID_COLUMN_NAME)
                      .setType("STRING")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(MismatchedRecord.TABLE_NAME_COLUMN_NAME)
                      .setType("STRING")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(MismatchedRecord.MISMATCH_TYPE_COLUMN_NAME)
                      .setType("STRING")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(MismatchedRecord.RECORD_KEY_COLUMN_NAME)
                      .setType("STRING")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(MismatchedRecord.SOURCE_COLUMN_NAME)
                      .setType("STRING")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(MismatchedRecord.HASH_COLUMN_NAME)
                      .setType("STRING")
                      .setMode("REQUIRED")));

  public static final TableSchema TABLE_VALIDATION_STATS_SCHEMA =
      new TableSchema()
          .setFields(
              Lists.newArrayList(
                  new TableFieldSchema()
                      .setName(TableValidationStats.RUN_ID_COLUMN_NAME)
                      .setType("STRING")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(TableValidationStats.TABLE_NAME_COLUMN_NAME)
                      .setType("STRING")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(TableValidationStats.STATUS_COLUMN_NAME)
                      .setType("STRING")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(TableValidationStats.SOURCE_ROW_COUNT_COLUMN_NAME)
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(TableValidationStats.DESTINATION_ROW_COUNT_COLUMN_NAME)
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(TableValidationStats.MATCHED_ROW_COUNT_COLUMN_NAME)
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(TableValidationStats.MISMATCH_ROW_COUNT_COLUMN_NAME)
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(TableValidationStats.START_TIMESTAMP_COLUMN_NAME)
                      .setType("TIMESTAMP")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(TableValidationStats.END_TIMESTAMP_COLUMN_NAME)
                      .setType("TIMESTAMP")
                      .setMode("REQUIRED")));

  public static final TableSchema VALIDATION_SUMMARY_SCHEMA =
      new TableSchema()
          .setFields(
              Lists.newArrayList(
                  new TableFieldSchema()
                      .setName(ValidationSummary.RUN_ID_COLUMN_NAME)
                      .setType("STRING")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(ValidationSummary.SOURCE_DATABASE_COLUMN_NAME)
                      .setType("STRING")
                      .setMode("NULLABLE"),
                  new TableFieldSchema()
                      .setName(ValidationSummary.DESTINATION_DATABASE_COLUMN_NAME)
                      .setType("STRING")
                      .setMode("NULLABLE"),
                  new TableFieldSchema()
                      .setName(ValidationSummary.STATUS_COLUMN_NAME)
                      .setType("STRING")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(ValidationSummary.TOTAL_TABLES_VALIDATED_COLUMN_NAME)
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(ValidationSummary.TABLES_WITH_MISMATCHES_COLUMN_NAME)
                      .setType("STRING")
                      .setMode("NULLABLE"),
                  new TableFieldSchema()
                      .setName(ValidationSummary.TOTAL_ROWS_MATCHED_COLUMN_NAME)
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(ValidationSummary.TOTAL_ROWS_MISMATCHED_COLUMN_NAME)
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(ValidationSummary.START_TIMESTAMP_COLUMN_NAME)
                      .setType("TIMESTAMP")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(ValidationSummary.END_TIMESTAMP_COLUMN_NAME)
                      .setType("TIMESTAMP")
                      .setMode("REQUIRED")));
}
