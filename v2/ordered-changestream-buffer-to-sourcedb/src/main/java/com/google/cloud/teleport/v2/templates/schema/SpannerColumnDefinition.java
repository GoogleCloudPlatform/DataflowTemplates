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

/**
 * SpannerColumnDefinition object to store Spanner table name and column name mapping information.
 */
public class SpannerColumnDefinition implements Serializable {

  /** Represents the name of the Spanner column. */
  private final String name;

  private final ColumnType t;

  public SpannerColumnDefinition(String name, ColumnType type) {
    this.name = name;
    this.t = type;
  }

  public String getName() {
    return name;
  }

  public ColumnType getType() {
    return t;
  }

  public String toString() {
    return String.format("{ 'name': '%s' , 'type': '%s'}", name, t);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof SpannerColumnDefinition)) {
      return false;
    }
    final SpannerColumnDefinition other = (SpannerColumnDefinition) o;
    return this.name.equals(other.name);
  }
}
