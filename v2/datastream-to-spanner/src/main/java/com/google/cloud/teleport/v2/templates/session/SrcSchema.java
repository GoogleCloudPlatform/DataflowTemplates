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
package com.google.cloud.teleport.v2.templates.session;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** SrcSchema object to store Spanner table name and column name mapping information. */
public class SrcSchema implements Serializable {

  /** Represents the name of the Source table. */
  private final String name;

  /** Represents the name of the Source schema. */
  private final String schema;

  /** List of all the column IDs in the same order as in the Spanner table. */
  private final String[] colIds;

  /** Maps the column ID to the column name. */
  private final Map<String, ColumnDef> colDefs;

  public SrcSchema(String name, String schema, String[] colIds, Map<String, ColumnDef> colDefs) {
    this.name = name;
    this.schema = schema;
    this.colIds = (colIds == null) ? (new String[] {}) : colIds;
    this.colDefs = (colDefs == null) ? (new HashMap<String, ColumnDef>()) : colDefs;
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

  public Map<String, ColumnDef> getColDefs() {
    return colDefs;
  }

  public String toString() {
    return String.format(
        "{ 'name': '%s', 'schema': '%s', 'colIds': '%s', 'colDefs': '%s' }",
        name, schema, colIds, colDefs);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof SrcSchema)) {
      return false;
    }
    final SrcSchema other = (SrcSchema) o;
    return this.name.equals(other.name)
        && this.schema.equals(other.schema)
        && Arrays.equals(this.colIds, other.colIds)
        && this.colDefs.equals(other.colDefs);
  }
}
