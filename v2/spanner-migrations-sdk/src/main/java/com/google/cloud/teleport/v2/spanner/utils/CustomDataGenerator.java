/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.spanner.utils;

import java.io.Serializable;

/** Interface for custom user-provided data generation logic. */
public interface CustomDataGenerator extends Serializable {
  /**
   * Sentinel object to be returned by {@link #generate} to indicate that an explicit null value
   * should be set instead of using default generator.
   */
  Object EXPLICIT_NULL = "CUSTOM_DATA_GENERATOR_EXPLICIT_NULL_SENTINEL";

  /**
   * Generates a custom value for the given column.
   *
   * @param tableName The table name.
   * @param columnName The column name.
   * @return The generated value. Return {@link #EXPLICIT_NULL} to emit an actual null value, or
   *     return null to fall back to the default generator.
   */
  Object generate(String tableName, String columnName);
}
