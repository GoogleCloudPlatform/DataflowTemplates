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
package com.google.cloud.teleport.v2.spanner.migrations.schema;

import java.io.Serializable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class NoopSchemaOverridesParser implements ISchemaOverridesParser, Serializable {

  /**
   * Gets the spanner table name given the source table name, or source table name if no override is
   * configured.
   *
   * @param sourceTableName The source table name
   * @return The overridden spanner table name
   */
  @Override
  public String getTableOverride(String sourceTableName) {
    return sourceTableName;
  }

  /**
   * Gets the spanner column name given the source table name, or the source column name if override
   * is configured.
   *
   * @param sourceTableName the source table name for which column name is overridden
   * @param sourceColumnName the source column name being overridden
   * @return A pair of spannerTableName and spannerColumnName
   */
  @Override
  public Pair<String, String> getColumnOverride(String sourceTableName, String sourceColumnName) {
    return new ImmutablePair<>(sourceTableName, sourceColumnName);
  }
}
