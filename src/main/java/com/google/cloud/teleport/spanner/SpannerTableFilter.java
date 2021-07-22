/*
 * Copyright (C) 2021 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.spanner;

import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.Table;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper class that provides a static function for filter a Ddl based on a provided list of table
 * names.
 */
public class SpannerTableFilter {

  /**
   * Given a list of table names and database Ddl, returns a Collection of Tables from the Ddl that
   * contains only the Tables with the corresponding table names.
   */
  static Collection<Table> getFilteredTables(Ddl ddl, List<String> tables) {
    Collection<Table> allTables = ddl.allTables();
    // If there are no tables provided, return all the tables
    if (tables.isEmpty()) {
      return allTables;
    }

    Set<Table> filteredTables =
        allTables.stream()
            .distinct()
            .filter(t -> tables.contains(t.name()))
            .collect(Collectors.toSet());

    return filteredTables;
  }
}
