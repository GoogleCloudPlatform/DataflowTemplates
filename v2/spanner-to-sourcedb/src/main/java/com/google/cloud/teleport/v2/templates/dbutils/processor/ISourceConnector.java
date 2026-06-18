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
package com.google.cloud.teleport.v2.templates.dbutils.processor;

import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import java.util.List;

/**
 * Interface representing a connector to the external source/destination database (e.g., MySQL,
 * PostgreSQL, Cassandra) involved in the Spanner migration.
 *
 * <p>An implementation of this interface encapsulates all dialect-specific configurations,
 * connection management, metadata discovery, and validation logic required by the pipeline to
 * integrate with the target database.
 */
public interface ISourceConnector {

  /**
   * Returns the dialect-specific DML Generator for this database.
   *
   * @return An implementation of {@link IDMLGenerator} to generate INSERT/UPDATE/DELETE queries.
   */
  IDMLGenerator getDmlGenerator();

  /**
   * Returns the dialect-specific Connection Helper for this database.
   *
   * @return An implementation of {@link IConnectionHelper} to manage connection pools.
   */
  IConnectionHelper getConnectionHelper();

  /**
   * Constructs and returns the JDBC or connection URL for a given shard.
   *
   * @param shard The shard configuration containing host, port, and database name.
   * @return The connection URL string.
   */
  String getConnectionUrl(Shard shard);

  /**
   * Returns a dialect-specific Data Access Object (DAO) for a given shard.
   *
   * @param shard The shard configuration to initialize the DAO with.
   * @return An implementation of {@link IDao} for executing statements against the shard.
   */
  IDao getDao(Shard shard);

  /**
   * Initializes the connection helper and connection pools for all shards.
   *
   * @param shards The list of all shards to establish connection pools for.
   * @param maxConnections The maximum number of connections allowed in the pool.
   */
  void initConnectionHelper(List<Shard> shards, int maxConnections);

  /**
   * Scans the database schema for a given shard and returns its structured schema.
   *
   * @param shard The shard to scan.
   * @return The discovered {@link SourceSchema}.
   */
  SourceSchema getSourceSchema(Shard shard);

  /**
   * Validates that the target database shards are not in read-only mode, ensuring they are
   * writeable by the replication pipeline.
   *
   * @param shards The list of shards to validate.
   * @throws RuntimeException if any shard is determined to be read-only.
   */
  void validateNotReadOnly(List<Shard> shards);

  /**
   * Returns whether this database type supports sharding.
   *
   * @return true if sharding is supported (e.g., MySQL, PostgreSQL), false otherwise (e.g.,
   *     Cassandra).
   */
  boolean isShardingSupported();
}
