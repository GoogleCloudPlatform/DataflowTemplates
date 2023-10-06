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
import java.util.Objects;

/** SourceColumnType object to store Source column type. */
public class SourceColumnType implements Serializable {

  /** Represents the name of the Source column. */
  private final String name;

  /** Represents mods of the Source Column (ex: varchar(30) will have mods as [30]). */
  private final Long[] mods;

  /** Represents array bounds(if any) of the type. */
  private final Long[] arrayBounds;

  public SourceColumnType(String name, Long[] mods, Long[] arrayBounds) {
    this.name = name;
    this.mods = mods;
    this.arrayBounds = arrayBounds;
  }

  public String getName() {
    return name;
  }

  public Long[] getMods() {
    return mods;
  }

  public Long[] getArrayBounds() {
    return arrayBounds;
  }

  public String toString() {
    return String.format(
        "{ 'name': '%s' , 'mods' :  '%s', 'arrayBounds' :  '%s' }",
        name, Arrays.toString(mods), Arrays.toString(arrayBounds));
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof SourceColumnType)) {
      return false;
    }
    final SourceColumnType other = (SourceColumnType) o;
    return this.name.equals(other.name)
        && Arrays.equals(this.mods, other.mods)
        && Arrays.equals(this.arrayBounds, other.arrayBounds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, Arrays.hashCode(mods), Arrays.hashCode(arrayBounds));
  }
}
