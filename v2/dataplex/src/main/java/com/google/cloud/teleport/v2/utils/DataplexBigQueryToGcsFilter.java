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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.v2.options.DataplexBigQueryToGcsOptions;
import com.google.cloud.teleport.v2.utils.DataplexWriteDisposition.WriteDispositionException;
import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.cloud.teleport.v2.values.BigQueryTablePartition;
import com.google.common.base.Splitter;
import com.google.re2j.Pattern;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Filter to exclude unwanted tables and partitions. */
public class DataplexBigQueryToGcsFilter implements BigQueryMetadataLoader.Filter {
  private static final Logger LOG = LoggerFactory.getLogger(DataplexBigQueryToGcsFilter.class);
  private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

  private final Instant maxLastModifiedTime;
  private final Set<String> includeTables;
  private final Pattern includePartitions;
  private final String writeDisposition;
  private final String fileSuffix;
  private final List<String> existingTargetFiles;
  private final BigQueryToGcsDirectoryNaming directoryNaming;

  public DataplexBigQueryToGcsFilter(
      DataplexBigQueryToGcsOptions options, List<String> existingTargetFiles) {
    String dateTime = options.getExportDataModifiedBeforeDateTime();
    if (dateTime != null && !dateTime.isEmpty()) {
      if (dateTime.startsWith("-P") || dateTime.startsWith("-p")) {
        this.maxLastModifiedTime = Instant.now().plus(Duration.parse(dateTime).toMillis());
      } else {
        this.maxLastModifiedTime =
            Instant.parse(dateTime, ISODateTimeFormat.dateOptionalTimeParser());
      }
    } else {
      this.maxLastModifiedTime = null;
    }

    String tableRefs = options.getTables();
    if (tableRefs != null && !tableRefs.isEmpty()) {
      List<String> tableRefList = SPLITTER.splitToList(tableRefs);
      checkArgument(
          !tableRefList.isEmpty(),
          "Got an non-empty tables param '%s', but couldn't parse it into a valid table list,"
              + " please check its format.",
          tableRefs);
      this.includeTables = new HashSet<>(tableRefList);
    } else {
      this.includeTables = null;
    }

    String partitionRegExp = options.getPartitionIdRegExp();
    if (partitionRegExp != null && !partitionRegExp.isEmpty()) {
      this.includePartitions = Pattern.compile(partitionRegExp);
    } else {
      this.includePartitions = null;
    }

    this.writeDisposition = options.getWriteDisposition().getWriteDispositionOption();
    this.fileSuffix = options.getFileFormat().getFileSuffix();
    this.existingTargetFiles = existingTargetFiles;
    this.directoryNaming = new BigQueryToGcsDirectoryNaming(options.getEnforceSamePartitionKey());
  }

  private boolean shouldSkipTableName(BigQueryTable.Builder table) {
    if (includeTables != null && !includeTables.contains(table.getTableName())) {
      return true;
    }
    return false;
  }

  private boolean shouldSkipFile(String table, String partition, String path) {
    String identifier = partition == null ? table : table + "$" + partition;
    switch (writeDisposition) {
      case "FAIL":
        throw new WriteDispositionException(
            String.format(
                "Target File %s exists for %s. Failing according to writeDisposition = %s.",
                path, identifier, writeDisposition));
      case "SKIP":
        LOG.info(
            "Target File {} exists for {}. Skipping according to writeDisposition = {}.",
            path,
            identifier,
            writeDisposition);
        return true;
      case "OVERWRITE":
        LOG.info(
            "Target File {} exists for {}. Overwriting according to writeDisposition = {}.",
            path,
            identifier,
            writeDisposition);
        return false;
      default:
        throw new UnsupportedOperationException(
            writeDisposition + " writeDisposition not implemented");
    }
  }

  @Override
  public boolean shouldSkipUnpartitionedTable(BigQueryTable.Builder table) {
    if (shouldSkipTableName(table)) {
      return true;
    }
    // Check the last modified time only for NOT partitioned table.
    // If a table is partitioned, we check the last modified time on partition level only.
    if (maxLastModifiedTime != null
        // BigQuery timestamps are in microseconds so / 1000.
        && maxLastModifiedTime.isBefore(table.getLastModificationTime() / 1000)) {
      return true;
    }
    // Check if the target file already exists.
    String expectedTargetPath = tableTargetFileName(table);
    if (existingTargetFiles.contains(expectedTargetPath)) {
      return shouldSkipFile(table.getTableName(), null, expectedTargetPath);
    }
    return false;
  }

  @Override
  public boolean shouldSkipPartitionedTable(
      BigQueryTable.Builder table, List<BigQueryTablePartition> partitions) {
    if (shouldSkipTableName(table)) {
      return true;
    }
    if (partitions.isEmpty()) {
      LOG.info(
          "Skipping table {}: "
              + "table is partitioned, but no eligible partitions found => nothing to export.",
          table.getTableName());
      return true;
    }
    return false;
  }

  @Override
  public boolean shouldSkipPartition(
      BigQueryTable.Builder table, BigQueryTablePartition partition) {
    if (maxLastModifiedTime != null
        // BigQuery timestamps are in microseconds so / 1000.
        && maxLastModifiedTime.isBefore(partition.getLastModificationTime() / 1000)) {
      return true;
    }
    if (includePartitions != null && !includePartitions.matches(partition.getPartitionName())) {
      LOG.info(
          "Skipping partition {} not matching regexp: {}",
          partition.getPartitionName(),
          includePartitions.pattern());
      return true;
    }
    // Check if target file already exists.
    String expectedTargetPath = partitionTargetFileName(table, partition);
    if (existingTargetFiles.contains(expectedTargetPath)) {
      return shouldSkipFile(table.getTableName(), partition.getPartitionName(), expectedTargetPath);
    }
    return false;
  }

  public String tableTargetFileName(BigQueryTable.Builder table) {
    String dirName = directoryNaming.getTableDirectory(table.getTableName());
    String fileName =
        new BigQueryToGcsFileNaming(fileSuffix, table.getTableName())
            .getFilename(null, null, 0, 0, null);
    return dirName + "/" + fileName;
  }

  public String partitionTargetFileName(
      BigQueryTable.Builder table, BigQueryTablePartition partition) {
    String dirName =
        directoryNaming.getPartitionDirectory(
            table.getTableName(), partition.getPartitionName(), table.getPartitioningColumn());
    String fileName =
        new BigQueryToGcsFileNaming(fileSuffix, table.getTableName(), partition.getPartitionName())
            .getFilename(null, null, 0, 0, null);
    return dirName + "/" + fileName;
  }
}
