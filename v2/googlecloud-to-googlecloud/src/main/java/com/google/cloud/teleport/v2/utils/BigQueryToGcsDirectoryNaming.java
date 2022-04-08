/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.utils;

/** Generates subdirectory names used for storing BigQuery tables/partitions in GCS. */
public class BigQueryToGcsDirectoryNaming {
  private static final String PARTITION_ID_RENAME_SUFFIX = "_pid";

  private final boolean enforceSamePartitionKey;

  public BigQueryToGcsDirectoryNaming(boolean enforceSamePartitionKey) {
    this.enforceSamePartitionKey = enforceSamePartitionKey;
  }

  public String getTableDirectory(String tableName) {
    return tableName;
  }

  public String getPartitionDirectory(
      String tableName, String partitionName, String partitioningColumn) {
    return String.format(
        "%s/%s%s=%s",
        tableName,
        partitioningColumn,
        enforceSamePartitionKey ? "" : PARTITION_ID_RENAME_SUFFIX,
        partitionName);
  }
}
