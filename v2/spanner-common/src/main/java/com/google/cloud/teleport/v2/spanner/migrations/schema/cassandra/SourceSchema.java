/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.schema.cassandra;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Map;

/** Represents an immutable source schema. */
@AutoValue
public abstract class SourceSchema implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Factory method to create a new instance of SourceSchema.
   *
   * @param tables the map of source tables to initialize.
   * @return a new immutable SourceSchema instance.
   */
  public static SourceSchema create(Map<String, SourceTable> tables) {
    return new AutoValue_SourceSchema(Map.copyOf(tables));
  }

  /**
   * Gets the source schema as an unmodifiable map.
   *
   * @return the unmodifiable source schema map.
   */
  public abstract Map<String, SourceTable> tables();
}
