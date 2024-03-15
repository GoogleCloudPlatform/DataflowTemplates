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
import java.util.Objects;

/** SpannerColumnType object to store Source column type. */
public class SpannerColumnType implements Serializable {

  /** Represents the Spanner column type. */
  private final String name;

  /** Represents if the column is an array. */
  private final Boolean isArray;

  public SpannerColumnType(String name, Boolean isArray) {
    this.name = name;
    this.isArray = isArray;
  }

  public String getName() {
    return name;
  }

  public Boolean getIsArray() {
    return isArray;
  }

  public String toString() {
    return String.format("{ 'name': '%s' , 'isArray' :  '%s' }", name, isArray);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof SpannerColumnType)) {
      return false;
    }
    final SpannerColumnType other = (SpannerColumnType) o;
    return this.name.equals(other.name) && this.isArray.equals(other.isArray);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, isArray);
  }
}
