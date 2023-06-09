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
import java.util.Map;

/** CreateTable object to store Spanner table name and column name mapping information. */
public class CreateTable implements Serializable {

  /** Represents the name of the Spanner table. */
  private final String name;

  /** List of all the column IDs in the same order as in the Spanner table. */
  private final String[] colIds;

  /** Maps the column ID to the column name. */
  private final Map<String, ColumnDef> colDefs;

  public CreateTable(String name, String[] colIds, Map<String, ColumnDef> colDefs) {
    this.name = name;
    this.colIds = colIds;
    this.colDefs = colDefs;
  }

  public String getName() {
    return name;
  }

  public String[] getColIds() {
    return colIds;
  }

  public Map<String, ColumnDef> getColDefs() {
    return colDefs;
  }

  public String toString() {
    return String.format(
        "{ 'name': '%s', 'colIds': '%s', 'colDefs': '%s' }", name, colIds, colDefs);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof CreateTable)) {
      return false;
    }
    final CreateTable other = (CreateTable) o;
    return this.name.equals(other.name)
        && Arrays.equals(this.colIds, other.colIds)
        && this.colDefs.equals(other.colDefs);
  }
}
