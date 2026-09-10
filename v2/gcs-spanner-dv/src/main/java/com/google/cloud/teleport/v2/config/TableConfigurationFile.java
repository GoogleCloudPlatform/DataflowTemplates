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
package com.google.cloud.teleport.v2.config;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** POJO representing the table configuration JSON file. */
public class TableConfigurationFile implements Serializable {

  private final List<String> tableNames;

  /**
   * Future Extensibility: Map of Source Table Name -> Table-specific configuration.
   *
   * <p>Note: The `tableNames` list remains the absolute source of truth for the exhaustive list of
   * tables to be validated. This map is strictly for providing advanced configurations (e.g.,
   * column filtering, sampling) for a subset of those tables. Tables cannot be implicitly included
   * for validation by solely appearing in this map; they MUST be explicitly listed in `tableNames`.
   *
   * <p>This is currently a placeholder and is not yet processed by the pipeline logic.
   */
  private final Map<String, TableLevelConfig> optionalConfigurations;

  public TableConfigurationFile(
      List<String> tableNames, Map<String, TableLevelConfig> optionalConfigurations) {
    this.tableNames = tableNames;
    this.optionalConfigurations = optionalConfigurations;
  }

  public List<String> getTableNames() {
    return tableNames;
  }

  public Map<String, TableLevelConfig> getOptionalConfigurations() {
    return optionalConfigurations;
  }
}
