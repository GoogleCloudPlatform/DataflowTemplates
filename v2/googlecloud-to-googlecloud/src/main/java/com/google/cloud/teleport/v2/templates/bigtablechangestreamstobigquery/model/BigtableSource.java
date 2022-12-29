package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class BigtableSource implements Serializable {

  private final String instanceId;
  private final String tableId;
  private final String charset;
  private final Set<String> columnFamiliesToIgnore;
  private final Set<String> columnsToIgnore;

  public BigtableSource(String instanceId, String tableId, String charset,
      String ignoreColumnFamilies, String ignoreColumns) {
    this.instanceId = instanceId;
    this.tableId = tableId;
    this.charset = charset;

    if (StringUtils.isBlank(ignoreColumnFamilies)) {
      this.columnFamiliesToIgnore = Collections.emptySet();
    } else {
      this.columnFamiliesToIgnore = Arrays.stream(
          ignoreColumnFamilies.trim().split("[\\s]*,[\\s]*")
      ).collect(Collectors.toSet());
    }

    if (StringUtils.isBlank(ignoreColumns)) {
      this.columnsToIgnore = Collections.emptySet();
    } else {
      this.columnsToIgnore = Arrays.stream(
          ignoreColumns.trim().split("[\\s]*,[\\s]*")
      ).collect(Collectors.toSet());
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
}
