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
                  new TableFieldSchema().setName("run_id").setType("STRING").setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("table_name")
                      .setType("STRING")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("mismatch_type")
                      .setType("STRING")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("record_key")
                      .setType("STRING")
                      .setMode("REQUIRED"),
                  new TableFieldSchema().setName("source").setType("STRING").setMode("REQUIRED"),
                  new TableFieldSchema().setName("hash").setType("STRING").setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("mismatched_columns")
                      .setType("STRING")
                      .setMode("NULLABLE")));

  public static final TableSchema TABLE_VALIDATION_STATS_SCHEMA =
      new TableSchema()
          .setFields(
              Lists.newArrayList(
                  new TableFieldSchema().setName("run_id").setType("STRING").setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("table_name")
                      .setType("STRING")
                      .setMode("REQUIRED"),
                  new TableFieldSchema().setName("status").setType("STRING").setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("source_row_count")
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("destination_row_count")
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("matched_row_count")
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("mismatch_row_count")
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("start_timestamp")
                      .setType("TIMESTAMP")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("end_timestamp")
                      .setType("TIMESTAMP")
                      .setMode("REQUIRED")));

  public static final TableSchema VALIDATION_SUMMARY_SCHEMA =
      new TableSchema()
          .setFields(
              Lists.newArrayList(
                  new TableFieldSchema().setName("run_id").setType("STRING").setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("source_database")
                      .setType("STRING")
                      .setMode("NULLABLE"),
                  new TableFieldSchema()
                      .setName("destination_database")
                      .setType("STRING")
                      .setMode("NULLABLE"),
                  new TableFieldSchema().setName("status").setType("STRING").setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("total_tables_validated")
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("tables_with_mismatches")
                      .setType("STRING")
                      .setMode("NULLABLE"),
                  new TableFieldSchema()
                      .setName("total_rows_matched")
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("total_rows_mismatched")
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("start_timestamp")
                      .setType("TIMESTAMP")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName("end_timestamp")
                      .setType("TIMESTAMP")
                      .setMode("REQUIRED")));
}
