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
package com.google.cloud.teleport.v2.templates.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** Configuration class for schema overrides, used for deserializing HOCON/JSON config files. */
public class SchemaConfig implements Serializable {

  private Map<String, TableConfig> tables;

  public Map<String, TableConfig> getTables() {
    return tables;
  }

  public void setTables(Map<String, TableConfig> tables) {
    this.tables = tables;
  }

  public static Map<String, TableConfig> getTables(SchemaConfig config) {
    return (config != null && config.getTables() != null)
        ? config.getTables()
        : java.util.Collections.emptyMap();
  }

  public static class TableConfig implements Serializable {
    private Integer insertQps;
    private Integer updateQps;
    private Integer deleteQps;
    private Map<String, ColumnConfig> columns;
    private List<ForeignKeyConfig> foreignKeys;

    public Integer getInsertQps() {
      return insertQps;
    }

    public void setInsertQps(Integer insertQps) {
      this.insertQps = insertQps;
    }

    public Integer getUpdateQps() {
      return updateQps;
    }

    public void setUpdateQps(Integer updateQps) {
      this.updateQps = updateQps;
    }

    public Integer getDeleteQps() {
      return deleteQps;
    }

    public void setDeleteQps(Integer deleteQps) {
      this.deleteQps = deleteQps;
    }

    public Map<String, ColumnConfig> getColumns() {
      return columns;
    }

    public void setColumns(Map<String, ColumnConfig> columns) {
      this.columns = columns;
    }

    public List<ForeignKeyConfig> getForeignKeys() {
      return foreignKeys;
    }

    public void setForeignKeys(List<ForeignKeyConfig> foreignKeys) {
      this.foreignKeys = foreignKeys;
    }
  }

  public static class ColumnConfig implements Serializable {
    private Object fakerExpression;
    private Boolean skip;

    public Object getFakerExpression() {
      return fakerExpression;
    }

    public void setFakerExpression(Object fakerExpression) {
      this.fakerExpression = fakerExpression;
    }

    public Boolean getSkip() {
      return skip;
    }

    public void setSkip(Boolean skip) {
      this.skip = skip;
    }
  }

  public static class ForeignKeyConfig implements Serializable {
    private String name;
    private String referencedTable;
    private List<String> keyColumns;
    private List<String> referencedColumns;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getReferencedTable() {
      return referencedTable;
    }

    public void setReferencedTable(String referencedTable) {
      this.referencedTable = referencedTable;
    }

    public List<String> getKeyColumns() {
      return keyColumns;
    }

    public void setKeyColumns(List<String> keyColumns) {
      this.keyColumns = keyColumns;
    }

    public List<String> getReferencedColumns() {
      return referencedColumns;
    }

    public void setReferencedColumns(List<String> referencedColumns) {
      this.referencedColumns = referencedColumns;
    }
  }
}
