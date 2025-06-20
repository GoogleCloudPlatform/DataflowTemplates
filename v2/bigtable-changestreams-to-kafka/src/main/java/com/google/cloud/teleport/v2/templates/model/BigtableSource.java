/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/** Descriptor of the Cloud Bigtable source table where changes are captured from. */
public class BigtableSource implements Serializable {
  public static final String ANY_COLUMN_FAMILY = "*";

  private final String instanceId;
  private final String tableId;
  private final String charset;
  private final Set<String> columnFamiliesToIgnore;
  private final Map<String, Set<String>> ignoredColumnsMap;

  public BigtableSource(
      String instanceId,
      String tableId,
      String charset,
      String ignoreColumnFamilies,
      String ignoreColumns) {
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

    Set<String> columnsToIgnore;
    if (StringUtils.isBlank(ignoreColumns)) {
      columnsToIgnore = Collections.emptySet();
    } else {
      columnsToIgnore =
          Arrays.stream(ignoreColumns.trim().split("[\\s]*,[\\s]*")).collect(Collectors.toSet());
    }

    ignoredColumnsMap = new HashMap<>();
    for (String columnFamilyAndColumn : columnsToIgnore) {
      String[] parts = columnFamilyAndColumn.split(":", 2);
      String columnFamily = ANY_COLUMN_FAMILY;
      String columnName = columnFamilyAndColumn;
      if (parts.length == 2) {
        columnFamily = parts[0];
        if (StringUtils.isBlank(columnFamily)) {
          columnFamily = ANY_COLUMN_FAMILY;
        }
        columnName = parts[1];
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

  public Boolean isIgnoredColumnFamily(String columnFamily) {
    return columnFamiliesToIgnore.contains(columnFamily);
  }

  public boolean isIgnoredColumn(String columnFamily, String column) {
    Set<String> columnFamilies = ignoredColumnsMap.get(column);
    if (columnFamilies == null) {
      return false;
    }
    return columnFamilies.contains(columnFamily) || columnFamilies.contains(ANY_COLUMN_FAMILY);
  }
}
