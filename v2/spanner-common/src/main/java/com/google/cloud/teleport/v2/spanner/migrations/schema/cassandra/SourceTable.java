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
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.List;

/** Represents a source table schema with its name and associated columns. */
@AutoValue
public abstract class SourceTable implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance of SourceTable.
   *
   * @param name the name of the source table.
   * @param columns the list of columns in the source table.
   * @return a new immutable SourceTable instance.
   */
  public static SourceTable create(String name, List<SourceColumn> columns) {
    return new AutoValue_SourceTable(name, ImmutableList.copyOf(columns));
  }

  /**
   * Gets the name of the source table.
   *
   * @return the source table name.
   */
  public abstract String name();

  /**
   * Gets the list of columns in the source table.
   *
   * @return an immutable list of SourceColumn objects.
   */
  public abstract ImmutableList<SourceColumn> columns();
}
