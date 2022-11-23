/*
 * Copyright (C) 2022 Google LLC
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

/** ColumnDef object to store Spanner table name and column name mapping information. */
public class ColumnDef implements Serializable {

  /** Represents the name of the Spanner column. */
  private final String name;

  public ColumnDef(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public String toString() {
    return String.format("{ 'name': '%s' }", name);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof ColumnDef)) {
      return false;
    }
    final ColumnDef other = (ColumnDef) o;
    return this.name.equals(other.name);
  }
}
