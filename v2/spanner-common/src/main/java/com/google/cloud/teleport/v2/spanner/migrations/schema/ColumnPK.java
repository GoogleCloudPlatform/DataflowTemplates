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

/** ColumnPK object to store table PK for both Spanner and Source. */
public class ColumnPK implements Serializable {

  /** Represents the name of the Spanner column. */
  private final String colId;

  /** Represents the position of the col in the primary key. */
  private final int order;

  public ColumnPK(String colId, int order) {
    this.colId = colId;
    this.order = order;
  }

  public String getColId() {
    return colId;
  }

  public int getOrder() {
    return order;
  }

  public String toString() {
    return String.format("{ 'colId': '%s' , 'order': '%s' }", colId, order);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof ColumnPK)) {
      return false;
    }
    final ColumnPK other = (ColumnPK) o;
    return this.colId.equals(other.colId) && this.order == other.order;
  }
}
