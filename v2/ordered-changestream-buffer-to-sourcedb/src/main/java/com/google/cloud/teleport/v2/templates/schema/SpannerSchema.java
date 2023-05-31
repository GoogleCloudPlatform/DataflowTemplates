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
package com.google.cloud.teleport.v2.templates.schema;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** SpannerSchema object to store Spanner table name and column name mapping information. */
public class SpannerSchema implements Serializable {

  /** Represents the name of the Source table. */
  private final String name;

  /** Represents the name of the Source schema. */
  private final String schema;

  /** List of all the column IDs in the same order as in the Spanner table. */
  private final String[] colIds;

  /** Maps the column ID to the column name. */
  private final Map<String, SpannerColumnDefinition> colDefs;

  private final ColumnPK[] primaryKeys;

  public SpannerSchema(
      String name,
      String schema,
      String[] colIds,
      Map<String, SpannerColumnDefinition> colDefs,
      ColumnPK[] primaryKeys) {
    this.name = name;
    this.schema = schema;
    this.colIds = (colIds == null) ? (new String[] {}) : colIds;
    this.colDefs = (colDefs == null) ? (new HashMap<String, SpannerColumnDefinition>()) : colDefs;
    this.primaryKeys = (primaryKeys == null) ? (new ColumnPK[] {}) : primaryKeys;
  }

  public String getName() {
    return name;
  }

  public String getSchema() {
    return schema;
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
        "{ 'name': '%s', 'schema': '%s', 'colIds': '%s', 'colDefs': '%s','primaryKeys': '%s' }",
        name, schema, colIds, colDefs, pvalues);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof SpannerSchema)) {
      return false;
    }
    final SpannerSchema other = (SpannerSchema) o;
    return this.name.equals(other.name)
        && this.schema.equals(other.schema)
        && Arrays.equals(this.colIds, other.colIds)
        && this.colDefs.equals(other.colDefs);
  }
}
