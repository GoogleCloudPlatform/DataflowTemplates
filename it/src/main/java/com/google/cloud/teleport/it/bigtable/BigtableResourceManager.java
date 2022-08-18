/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.it.bigtable;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.collect.ImmutableList;

public interface BigtableResourceManager {

    /**
     * Creates a Bigtable instance in which all clusters, nodes and tables will exist.
     *
     * @param clusters Collection of BigtableResourceManagerCluster objects to associate with the given Bigtable instance.
     */
    void createInstance(Iterable<BigtableResourceManagerCluster> clusters);

    /**
     * Creates a table within the current instance given a table ID.
     *
     * <p>Note: Implementations may do instance creation here, if one does not already exist.
     *
     * @param tableId The id of the table.
     * @param columnFamilies A collection of column family names for the table.
     */
    void createTable(String tableId, Iterable<String> columnFamilies);

    /**
     * Writes a given row into a table. This method requires {@link
     * BigtableResourceManager#createTable(String, Iterable)} to be called for the target table beforehand.
     *
     * @param tableRow A mutation object representing the table row.
     * @throws RuntimeException if method is called after resources have been cleaned up or if
     *     the manager object has no instance or database.
     */
    void write(RowMutation tableRow) throws RuntimeException;

    /**
     * Writes a collection of table rows into one or more tables. This method requires {@link
     * BigtableResourceManager#createTable(String, Iterable)} to be called for the target table beforehand.
     *
     * @param tableRows A collection of mutation objects representing table rows.
     * @throws RuntimeException if method is called after resources have been cleaned up or if
     *     the manager object has no instance or database.
     */
    void write(Iterable<RowMutation> tableRows) throws RuntimeException;

    /**
     * Reads all the rows in a table.This method requires {@link
     * BigtableResourceManager#createTable(String, Iterable)} to be called for the target table beforehand.
     *
     * @param tableId The id of table to read rows from.
     * @return A ResultSet object containing all the rows in the table.
     * @throws RuntimeException if method is called after resources have been cleaned up or if
     *     the manager object has no instance or database.
     */
    ImmutableList<Row> readTable(String tableId) throws RuntimeException;

    /**
     * Deletes all created resources (instance, database, and tables) and cleans up all Spanner
     * sessions, making the manager object unusable.
     */
    void cleanupAll();
}
