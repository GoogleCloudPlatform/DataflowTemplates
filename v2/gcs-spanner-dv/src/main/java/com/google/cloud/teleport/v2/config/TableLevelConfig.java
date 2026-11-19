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
package com.google.cloud.teleport.v2.config;

import java.io.Serializable;

/**
 * POJO representing advanced configurations for a specific table.
 *
 * <p>Currently supports {@code spannerQuery}, the query used to read the table from Spanner (for
 * example, to restrict it to the selected {@code shardIds}). This is intended to support further
 * features like column-level validation or deterministic sampling in the future.
 */
public class TableLevelConfig implements Serializable {

  /** The Spanner read query for this table, or {@code null} when not configured. */
  // Future Extensibility: accept a list of non-overlapping queries (one ReadOperation each, same
  // table tag) so a large shard subset can stay within Spanner's query limits
  // (https://docs.cloud.google.com/spanner/quotas#query-limits).
  private final String spannerQuery;

  // Example future fields:
  // private List<String> columnsToValidate;
  // private SamplingConfig sampling;

  public TableLevelConfig(String spannerQuery) {
    this.spannerQuery = spannerQuery;
  }

  public String getSpannerQuery() {
    return spannerQuery;
  }
}
