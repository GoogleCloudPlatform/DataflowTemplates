/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model;

import com.google.cloud.bigquery.StandardSQLTypeName;

/**
 * Static metadata class for all changelog columns.
 */
public enum ChangelogColumn {
  ROW_KEY_STRING("row_key", StandardSQLTypeName.STRING.name(), true, false),
  ROW_KEY_BYTES("row_key", StandardSQLTypeName.BYTES.name(), true, false),
  MOD_TYPE("mod_type", StandardSQLTypeName.STRING.name(), true, false),
  COMMIT_TIMESTAMP("commit_timestamp", StandardSQLTypeName.TIMESTAMP.name(), true, false),
  COLUMN_FAMILY("column_family", StandardSQLTypeName.STRING.name(), true, false),
  COLUMN("column", StandardSQLTypeName.STRING.name(), false, false),
  TIMESTAMP("timestamp", StandardSQLTypeName.TIMESTAMP.name(), false, false),
  TIMESTAMP_NUM("timestamp", StandardSQLTypeName.INT64.name(), false, false),
  VALUE_STRING("value", StandardSQLTypeName.STRING.name(), false, false),
  VALUE_BYTES("value", StandardSQLTypeName.BYTES.name(), false, false),
  TIMESTAMP_FROM("timestamp_from", StandardSQLTypeName.TIMESTAMP.name(), false, false),
  TIMESTAMP_FROM_NUM("timestamp_from", StandardSQLTypeName.INT64.name(), false, false),
  TIMESTAMP_TO("timestamp_to", StandardSQLTypeName.TIMESTAMP.name(), false, false),
  TIMESTAMP_TO_NUM("timestamp_to", StandardSQLTypeName.INT64.name(), false, false),
  IS_GC("is_gc", "BOOL", false, true),
  SOURCE_INSTANCE("source_instance", StandardSQLTypeName.STRING.name(), false, true),
  SOURCE_CLUSTER("source_cluster", StandardSQLTypeName.STRING.name(), false, true),
  SOURCE_TABLE("source_table", StandardSQLTypeName.STRING.name(), false, true),
  TIEBREAKER("tiebreaker", StandardSQLTypeName.INT64.name(), false, true),
  BQ_COMMIT_TIMESTAMP("big_query_commit_timestamp", StandardSQLTypeName.TIMESTAMP.name(), false, true);

  private final String bqColumnName;
  private final String bqType;
  private final boolean required;
  private final boolean ignorable;

  ChangelogColumn(String bqColumnName, String bqType, boolean required, boolean ignorable) {
    this.bqColumnName = bqColumnName;
    this.bqType = bqType;
    this.required = required;
    this.ignorable = ignorable;
  }

  public String getBqColumnName() {
    return bqColumnName;
  }

  public String getBqType() {
    return bqType;
  }

  public boolean isRequired() {
    return required;
  }

  public boolean isIgnorable() {
    return ignorable;
  }
}
