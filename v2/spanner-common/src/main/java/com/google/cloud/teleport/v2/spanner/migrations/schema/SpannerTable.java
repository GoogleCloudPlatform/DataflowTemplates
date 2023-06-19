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
package com.google.cloud.teleport.v2.spanner.migrations.schema;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** SpannerTable object to store Spanner table name and column name mapping information. */
public class SpannerTable implements Serializable {

  /** Represents the name of the Spanner table. */
  private final String name;

  /** List of all the column IDs in the same order as in the Spanner table. */
  private final String[] colIds;

  /** Maps the column ID to the column definition. */
  private final Map<String, SpannerColumnDefinition> colDefs;

  private final ColumnPK[] primaryKeys;

  public SpannerTable(
      String name,
      String[] colIds,
      Map<String, SpannerColumnDefinition> colDefs,
      ColumnPK[] primaryKeys) {
    this.name = name;
    this.colIds = (colIds == null) ? (new String[] {}) : colIds;
    this.colDefs = (colDefs == null) ? (new HashMap<String, SpannerColumnDefinition>()) : colDefs;
    this.primaryKeys = (primaryKeys == null) ? (new ColumnPK[] {}) : primaryKeys;
  }

  public String getName() {
    return name;
  }

  public String[] getColIds() {
    return colIds;
  }

  public Map<String, SpannerColumnDefinition> getColDefs() {
    return colDefs;
  }

  public ColumnPK[] getPrimaryKeys() {
    return primaryKeys;
  }

  public Set<String> getPrimaryKeySet() {

    Set<String> response = new HashSet<>();
    if (primaryKeys != null && colDefs != null) {
      for (ColumnPK p : primaryKeys) {
        SpannerColumnDefinition pkColDef = colDefs.get(p.getColId());
        if (pkColDef != null) {
          response.add(pkColDef.getName());
        }
      }
    }
    return response;
  }

  public String toString() {
    String pvalues = "";
    if (primaryKeys != null) {
      for (int i = 0; i < primaryKeys.length; i++) {
        pvalues += primaryKeys[i].toString();
        pvalues += ",";
      }
    }
    return String.format(
        "{ 'name': '%s', colIds': '%s', 'colDefs': '%s','primaryKeys': '%s' }",
        name, colIds, colDefs, pvalues);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof SpannerTable)) {
      return false;
    }
    final SpannerTable other = (SpannerTable) o;
    return this.name.equals(other.name)
        && Arrays.equals(this.colIds, other.colIds)
        && this.colDefs.equals(other.colDefs)
        && Arrays.equals(this.primaryKeys, other.primaryKeys);
  }
}
