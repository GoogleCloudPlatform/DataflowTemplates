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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model;


/** Static metadata class for all changelog columns. */
public enum PubSubMessage {
  ROW_KEY_STRING("row_key"),
  ROW_KEY_BYTES("row_key"),
  MOD_TYPE("mod_type"),
  COMMIT_TIMESTAMP("commit_timestamp"),
  COLUMN_FAMILY("column_family"),
  COLUMN("column"),
  TIMESTAMP("timestamp"),
  TIMESTAMP_NUM("timestamp"),
  VALUE_STRING("value"),
  VALUE_BYTES("value"),
  TIMESTAMP_FROM("timestamp_from"),
  TIMESTAMP_FROM_NUM("timestamp_from"),
  TIMESTAMP_TO("timestamp_to"),
  TIMESTAMP_TO_NUM("timestamp_to"),
  IS_GC("is_gc"),
  SOURCE_INSTANCE("source_instance"),
  SOURCE_CLUSTER("source_cluster"),
  SOURCE_TABLE("source_table"),
  TIEBREAKER("tiebreaker");

  private final String fieldName;

  PubSubMessage(String fieldName) {
    this.fieldName = fieldName;
  }

  public String getFieldName() {
    return fieldName;
  }
}
