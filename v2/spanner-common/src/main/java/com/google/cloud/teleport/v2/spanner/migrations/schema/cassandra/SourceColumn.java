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

/**
 * Represents a source column schema with type, primary key information, and column kind. This class
 * provides immutable representation of a source column with necessary metadata.
 */
@AutoValue
public abstract class SourceColumn implements Serializable {

  /**
   * Creates a new instance of SourceColumn.
   *
   * @param name the name of the column, should not be null or empty.
   * @param kind the kind/type of the column (e.g., partition key, clustering key, regular).
   * @param sourceType the source type of the column (e.g., String, Int).
   * @param isPrimaryKey whether the column is a primary key.
   * @return a new immutable SourceColumn instance.
   */
  public static SourceColumn create(
      String name, String kind, String sourceType, boolean isPrimaryKey) {
    // Validate the parameters before creating the SourceColumn
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Column name cannot be null or empty.");
    }
    if (sourceType == null || sourceType.isEmpty()) {
      throw new IllegalArgumentException("Source type cannot be null or empty.");
    }

    return new AutoValue_SourceColumn(name, kind, sourceType, isPrimaryKey);
  }

  /**
   * Gets the name of the column.
   *
   * @return the name of the column.
   */
  public abstract String name();

  /**
   * Gets the kind/type of the column.
   *
   * @return the kind of the column.
   */
  public abstract String kind();

  /**
   * Gets the source type of the column.
   *
   * @return the source type of the column.
   */
  public abstract String sourceType();

  /**
   * Checks if the column is a primary key.
   *
   * @return {@code true} if the column is a primary key, otherwise {@code false}.
   */
  public abstract boolean isPrimaryKey();
}
