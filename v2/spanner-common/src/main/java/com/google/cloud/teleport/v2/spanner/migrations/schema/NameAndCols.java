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
import java.util.Map;
import java.util.Objects;

/** NameAndCols object to store Spanner table name and column name mapping information. */
public class NameAndCols implements Serializable {

  /** Represents the name/id of the table. */
  private final String name;

  /** Mapping the column names/Ids. */
  private final Map<String, String> cols;

  public NameAndCols(String name, Map<String, String> cols) {
    this.name = name;
    this.cols = cols;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getCols() {
    return cols;
  }

  public String toString() {
    return String.format("{ 'name': '%s', 'cols': '%s' }", name, cols);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof NameAndCols)) {
      return false;
    }
    final NameAndCols other = (NameAndCols) o;
    return this.name.equals(other.name) && this.cols.equals(other.cols);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, cols);
  }
}
