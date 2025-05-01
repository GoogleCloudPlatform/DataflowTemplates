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
package com.google.cloud.teleport.v2.utils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Instant;

/** Descriptor of the Cloud Bigtable source table where changes are captured from. */
public class BigtableSource implements Serializable {
  public static final String ANY_COLUMN_FAMILY = "*";

  private final String instanceId;
  private final String tableId;
  private final String charset;
  private final Set<String> columnFamiliesToIgnore;
  private final Set<String> columnsToIgnore;
  private final Map<String, Set<String>> ignoredColumnsMap;

  private final Instant startTimestamp;

  public BigtableSource(
      String instanceId,
      String tableId,
      String charset,
      String ignoreColumnFamilies,
      String ignoreColumns,
      Instant startTimestamp) {
    this.startTimestamp = startTimestamp;
    this.instanceId = instanceId;
    this.tableId = tableId;
    this.charset = charset;

    if (StringUtils.isBlank(ignoreColumnFamilies)) {
      this.columnFamiliesToIgnore = Collections.emptySet();
    } else {
      this.columnFamiliesToIgnore =
          Arrays.stream(ignoreColumnFamilies.trim().split("[\\s]*,[\\s]*"))
              .collect(Collectors.toSet());
    }

    if (StringUtils.isBlank(ignoreColumns)) {
      this.columnsToIgnore = Collections.emptySet();
    } else {
      this.columnsToIgnore =
          Arrays.stream(ignoreColumns.trim().split("[\\s]*,[\\s]*")).collect(Collectors.toSet());
    }

    ignoredColumnsMap = new HashMap<>();
    for (String columnFamilyAndColumn : columnsToIgnore) {
      int indexOfColon = columnFamilyAndColumn.indexOf(':');
      String columnFamily = ANY_COLUMN_FAMILY;
      String columnName = columnFamilyAndColumn;
      if (indexOfColon > 0) {
        columnFamily = columnFamilyAndColumn.substring(0, indexOfColon);
        if (StringUtils.isBlank(columnFamily)) {
          columnFamily = ANY_COLUMN_FAMILY;
        }
        columnName = columnFamilyAndColumn.substring(indexOfColon + 1);
      }

      Set<String> appliedToColumnFamilies =
          ignoredColumnsMap.computeIfAbsent(columnName, k -> new HashSet<>());
      appliedToColumnFamilies.add(columnFamily);
    }
  }

  public String getInstanceId() {
    return instanceId;
  }

  public String getTableId() {
    return tableId;
  }

  public String getCharset() {
    return charset;
  }

  public Set<String> getColumnFamiliesToIgnore() {
    return columnFamiliesToIgnore;
  }

  public Set<String> getColumnsToIgnore() {
    return columnsToIgnore;
  }

  public Instant getStartTimestamp() {
    return startTimestamp;
  }

  public Map<String, Set<String>> getIgnoredColumnsMap() {
    return ignoredColumnsMap;
  }
}
