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
package com.google.cloud.teleport.it.cassandra;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.teleport.it.common.ResourceManager;
import java.util.List;
import java.util.Map;

/** Interface for managing Cassandra resources in integration tests. */
public interface CassandraResourceManager extends ResourceManager {

  /**
   * Returns the name of the Database that this Cassandra manager will operate in.
   *
   * @return the name of the Cassandra Database.
   */
  String getKeyspaceName();

  /** Returns the host to connect to the Cassandra Database. */
  String getHost();

  /** Returns the port to connect to the Cassandra Database. */
  int getPort();

  /**
   * Execute the given statement on the managed keyspace.
   *
   * @param statement The statement to execute.
   * @return ResultSet from Cassandra.
   */
  ResultSet executeStatement(String statement);

  /**
   * Inserts the given Documents into a collection.
   *
   * <p>Note: Implementations may do collection creation here, if one does not already exist.
   *
   * @param collectionName The name of the collection to insert the documents into.
   * @param documents A list of documents to insert into the collection.
   * @return A boolean indicating whether the Documents were inserted successfully.
   * @throws CassandraResourceManagerException if there is an error inserting the documents.
   */
  boolean insertDocuments(String collectionName, List<Map<String, Object>> documents);

  /**
   * Reads all the Documents in a collection.
   *
   * @param collectionName The name of the collection to read from.
   * @return An iterable of all the Documents in the collection.
   * @throws CassandraResourceManagerException if there is an error reading the collection.
   */
  Iterable<Row> readTable(String collectionName);
}
