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

import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.threeten.bp.Instant;

/**
 * Pure-data side-output record describing a Bigtable SET_CELL whose transformer output exceeded the
 * configured byte budget. Routed to the severe dead-letter queue by the pipeline; serialized to
 * JSON by {@code OversizedValueDlqSanitizer}.
 */
@DefaultCoder(SerializableCoder.class)
public final class OversizedValue implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String rowKey;
  @Nullable private final Instant commitTimestamp;
  private final String columnFamily;
  private final String column;
  private final long rawBytes;
  private final long estimatedDecodedBytes;
  private final long maxBytes;
  private final String sourceInstance;
  @Nullable private final String sourceCluster;
  private final String sourceTable;

  public OversizedValue(
      String rowKey,
      @Nullable Instant commitTimestamp,
      String columnFamily,
      String column,
      long rawBytes,
      long estimatedDecodedBytes,
      long maxBytes,
      String sourceInstance,
      @Nullable String sourceCluster,
      String sourceTable) {
    this.rowKey = rowKey;
    this.commitTimestamp = commitTimestamp;
    this.columnFamily = columnFamily;
    this.column = column;
    this.rawBytes = rawBytes;
    this.estimatedDecodedBytes = estimatedDecodedBytes;
    this.maxBytes = maxBytes;
    this.sourceInstance = sourceInstance;
    this.sourceCluster = sourceCluster;
    this.sourceTable = sourceTable;
  }

  public String rowKey() {
    return rowKey;
  }

  @Nullable
  public Instant commitTimestamp() {
    return commitTimestamp;
  }

  public String columnFamily() {
    return columnFamily;
  }

  public String column() {
    return column;
  }

  public long rawBytes() {
    return rawBytes;
  }

  public long estimatedDecodedBytes() {
    return estimatedDecodedBytes;
  }

  public long maxBytes() {
    return maxBytes;
  }

  public String sourceInstance() {
    return sourceInstance;
  }

  @Nullable
  public String sourceCluster() {
    return sourceCluster;
  }

  public String sourceTable() {
    return sourceTable;
  }
}
