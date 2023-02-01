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

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.commons.lang3.Validate;

/** Static metadata class for columns not written to BigQuery. */
public enum TransientColumn {
  BQ_CHANGELOG_FIELD_NAME_ORIGINAL_PAYLOAD_JSON("_metadata_original_payload_json"),
  BQ_CHANGELOG_FIELD_NAME_ERROR("_metadata_error"),
  BQ_CHANGELOG_FIELD_NAME_RETRY_COUNT("_metadata_retry_count");

  private final String columnName;

  TransientColumn(String columnName) {
    this.columnName = columnName;
  }

  public String getColumnName() {
    return columnName;
  }

  public static boolean isTransientColumn(String columnName) {
    return COLUMN_NAMES.contains(columnName);
  }

  private static final Set<String> COLUMN_NAMES;

  static {
    COLUMN_NAMES =
        ImmutableSet.<String>builder()
            .add("_metadata_original_payload_json")
            .add("_metadata_error")
            .add("_metadata_retry_count")
            .build();

    for (TransientColumn column : TransientColumn.values()) {
      Validate.isTrue(COLUMN_NAMES.contains(column.getColumnName()));
    }
  }
}
