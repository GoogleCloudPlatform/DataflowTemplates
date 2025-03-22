/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CassandraDriverConfigLoader;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;

public class CassandraSourceSchemaReader {
  /**
   * Reads metadata for the specified Cassandra keyspace.
   *
   * <p>This method retrieves metadata from the Cassandra system schema, including table names,
   * column names, column types, and kinds, for the given keyspace. It uses a prepared statement for
   * safe and efficient execution.
   *
   * @param cassandraShard the shard information
   * @return A {@link ResultSet} containing the metadata rows with columns:
   *     <ul>
   *       <li>{@code table_name} - The name of the table.
   *       <li>{@code column_name} - The name of the column.
   *       <li>{@code type} - The data type of the column.
   *       <li>{@code kind} - The column kind (e.g., partition_key, clustering).
   *     </ul>
   *
   * @throws IllegalArgumentException If the provided keyspace name is {@code null} or empty.
   * @throws ConnectionException If a connection to the Cassandra database could not be established.
   * @throws Exception If any other unexpected error occurs during the operation.
   */
  public static ResultSet getInformationSchemaAsResultSet(CassandraShard cassandraShard)
      throws Exception {
    try (CqlSession session = createCqlSession(cassandraShard)) {
      String query =
          "SELECT table_name, column_name, type, kind FROM system_schema.columns WHERE keyspace_name = ?";

      PreparedStatement preparedStatement = session.prepare(query);
      BoundStatement boundStatement = preparedStatement.bind(cassandraShard.getKeySpaceName());
      return session.execute(boundStatement);
    }
  }

  /**
   * Creates a {@link CqlSession} for the given {@link CassandraShard}.
   *
   * @param cassandraShard The shard containing connection details.
   * @return A {@link CqlSession} instance.
   */
  private static CqlSession createCqlSession(CassandraShard cassandraShard) {
    CqlSessionBuilder builder = CqlSession.builder();
    DriverConfigLoader configLoader =
        CassandraDriverConfigLoader.fromOptionsMap(cassandraShard.getOptionsMap());
    builder.withConfigLoader(configLoader);
    return builder.build();
  }
}
