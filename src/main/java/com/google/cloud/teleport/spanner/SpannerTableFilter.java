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
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

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

    /* The rest of this function is for handling the export of all necessary related tables.
     * We first get a Queue for the initial unprocessed Tables. Then, we iteratively gather related
     * tables for each table in unprocessedTables. If a related table (either a parent or foreign
     * key table) has not been processed (i.e. added to the completedTables list) and it's not
     * all already been checked into the unprocessedTables Queue, then it is added to the Queue.
     * All completedTables are returned at the end to the caller. */
    Queue<Table> unprocessedTables = getTablesFromDdl(ddl, tables);

    // Completed Tables list
    Collection<Table> completedTables = Lists.newArrayList();

    // Iteratively gather related tables via BFS
    while (!unprocessedTables.isEmpty()) {
      Table currTable = unprocessedTables.remove();
      // Add currTable to completedTables before processing relationships.
      // Doing this avoids adding this table again to unprocessTables in the
      // corner case of self references.
      completedTables.add(currTable);
      Set<Table> parentTables = getParentTables(ddl, currTable);
      Collection<Table> foreignKeysTables = getForeignKeyTables(ddl, currTable);

      for (Table parentTable : parentTables) {
        if (!completedTables.contains(parentTable) && !unprocessedTables.contains(parentTable)) {
          // This table needs to be processed (and it's not already in unprocessedTables)
          unprocessedTables.add(parentTable);
        }
      }

      for (Table fkTable : foreignKeysTables) {
        if (!completedTables.contains(fkTable) && !unprocessedTables.contains(fkTable)) {
          // This table needs to be processed (and it's not already in unprocessedTables)
          unprocessedTables.add(fkTable);
        }
      }
    }

    // All necessary tables have been processed
    return completedTables;
  }

  /**
   * Given a list of table names and database Ddl, returns a Queue of Tables from the Ddl that
   * represent those table names.
   */
  private static Queue<Table> getTablesFromDdl(Ddl ddl, List<String> tableNames) {
    Queue<Table> tables = new LinkedList<Table>();

    for (String t : tableNames) {
      tables.add(ddl.table(t));
    }
    return tables;
  }

  /**
   * Given a Table names and database Ddl, returns a Set of Tables from the Ddl that
   * represent the parent/ancestors of that table.
   */
  private static Set<Table> getParentTables(Ddl ddl, Table table) {
    Set<Table> parentTables = new HashSet<Table>();

    // Get parent tables, if it exists, for the passed in table
    for (Table t : ddl.allTables()) {
      // Get its child tables
      Collection<Table> childTables = ddl.childTables(t.name());
      if (childTables.contains(table)) {
        // t is a parent table that needs to be exported
        parentTables.add(t);
      }
    }

    return parentTables;
  }

  /**
   * Given a Table names and database Ddl, returns a Collection of Tables from the Ddl that
   * represent all referenced tables for that table's foreign key constraints.
   */
  private static Collection<Table> getForeignKeyTables(Ddl ddl, Table table) {
    return ddl.allReferencedTables(table.name());
  }
}

