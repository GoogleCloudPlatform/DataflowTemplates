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

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Descriptor of BigQuery table destination.
 */
public class BigQueryDestination implements Serializable {

  private final String bigQueryProject;
  private final String bigQueryDataset;
  private final String bigQueryTableName;
  private final String bigQueryChangelogTablePartitionGranularity;
  private final Long bigQueryChangelogTablePartitionExpirationMs;
  private final boolean writeRowkeyAsBytes;
  private final boolean writeValueAsBytes;
  private final boolean writeNumericTimestamps;
  private final Set<String> changelogFieldsToIgnore;

  public BigQueryDestination(
      String bigQueryProject,
      String bigQueryDataset,
      String bigQueryTableName,
      boolean writeRowkeyAsBytes,
      boolean writeValuesAsBytes,
      boolean writeNumericTimestamps,
      String bigQueryChangelogTablePartitionGranularity,
      Long bigQueryChangelogTablePartitionExpirationMs,
      String bigQueryChangelogTableFieldsToIgnore
  ) {
    this.bigQueryProject = bigQueryProject;
    this.bigQueryDataset = bigQueryDataset;
    this.bigQueryTableName = bigQueryTableName;
    this.bigQueryChangelogTablePartitionGranularity = safeToUpperCase(
        bigQueryChangelogTablePartitionGranularity);
    this.bigQueryChangelogTablePartitionExpirationMs = bigQueryChangelogTablePartitionExpirationMs;
    this.writeRowkeyAsBytes = writeRowkeyAsBytes;
    this.writeValueAsBytes = writeValuesAsBytes;
    this.writeNumericTimestamps = writeNumericTimestamps;

    if (!StringUtils.isBlank(bigQueryChangelogTablePartitionGranularity)) {
      try {
        TimePartitioning.Type.valueOf(bigQueryChangelogTablePartitionGranularity);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Partition granularity not supported: '" + bigQueryChangelogTablePartitionGranularity
                + "'. Currently supported values: " +
                Arrays.toString(TimePartitioning.Type.values()));
      }
    }

    if (bigQueryChangelogTablePartitionExpirationMs != null && StringUtils.isBlank(
        bigQueryChangelogTablePartitionGranularity)) {
      throw new IllegalArgumentException("Partition expiration can only be used "
          + "when partition granularity is configured");
    }

    if (StringUtils.isBlank(bigQueryChangelogTableFieldsToIgnore)) {
      this.changelogFieldsToIgnore = Collections.emptySet();
    } else {
      this.changelogFieldsToIgnore = Arrays.stream(
          bigQueryChangelogTableFieldsToIgnore.trim().split("[\\s]*,[\\s]*")
      ).map(s -> s.toLowerCase(Locale.getDefault())).collect(Collectors.toSet());
    }

    for (ChangelogColumn column : ChangelogColumn.values()) {
      if (!column.isIgnorable() && changelogFieldsToIgnore.contains(column.getBqColumnName())) {
        throw new IllegalArgumentException("Column '" + column.getBqColumnName()
            + "' cannot be disabled by the pipeline configuration");
      }
    }

    Map<String, ChangelogColumn> bqColumnsToMetadata = new HashMap<>();
    for (ChangelogColumn column : ChangelogColumn.values()) {
      // there will be duplicate keys, but it's fine
      bqColumnsToMetadata.put(column.getBqColumnName(), column);
    }

    for (String columnRequestedToIgnore : changelogFieldsToIgnore) {
      if (!bqColumnsToMetadata.containsKey(columnRequestedToIgnore)) {
        throw new IllegalArgumentException(
            "Column '" + columnRequestedToIgnore + "' cannot be disabled and is not recognized");
      }
    }
  }

  private String safeToUpperCase(String val) {
    if (val != null) {
      return val.toUpperCase(Locale.getDefault());
    }
    return null;
  }

  public boolean isColumnEnabled(ChangelogColumn column) {
    switch (column) {
      case TIMESTAMP:
      case TIMESTAMP_FROM:
      case TIMESTAMP_TO:
        return !writeNumericTimestamps;
      case TIMESTAMP_NUM:
      case TIMESTAMP_FROM_NUM:
      case TIMESTAMP_TO_NUM:
        return writeNumericTimestamps;
      case ROW_KEY_STRING:
        return !writeRowkeyAsBytes;
      case ROW_KEY_BYTES:
        return writeRowkeyAsBytes;
      case VALUE_STRING:
        return !writeValueAsBytes;
      case VALUE_BYTES:
        return writeValueAsBytes;
      default:
        break;
    }

    return !column.isIgnorable() || !changelogFieldsToIgnore.contains(column.getBqColumnName());
  }

  public ImmutableSet<String> getIgnoredBigQueryColumnsNames() {
    Set<String> ignoredColumns = new HashSet<>();
    for (ChangelogColumn col : ChangelogColumn.values()) {
      if (col.isIgnorable() && changelogFieldsToIgnore.contains(col.getBqColumnName())) {
        ignoredColumns.add(col.getBqColumnName());
      }
    }
    return ImmutableSet.copyOf(ignoredColumns);
  }

  public boolean isPartitioned() {
    return StringUtils.isNotBlank(bigQueryChangelogTablePartitionGranularity);
  }

  public String getPartitionByColumnName() {
    return isPartitioned() ? ChangelogColumn.COMMIT_TIMESTAMP.getBqColumnName() : null;
  }

  public String getBigQueryChangelogTablePartitionType() {
    return bigQueryChangelogTablePartitionGranularity;
  }

  public Long getBigQueryChangelogTablePartitionExpirationMs() {
    return bigQueryChangelogTablePartitionExpirationMs;
  }

  public TableId getBigQueryTableId() {
    return TableId.of(bigQueryProject, bigQueryDataset, bigQueryTableName);
  }
}
