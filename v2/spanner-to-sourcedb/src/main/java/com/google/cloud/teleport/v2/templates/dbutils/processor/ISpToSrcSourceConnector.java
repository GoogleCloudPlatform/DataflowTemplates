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
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Interface representing a connector to the external source/destination database (e.g., MySQL,
 * PostgreSQL, Cassandra) involved in the Spanner migration.
 *
 * <p>An implementation of this interface encapsulates all dialect-specific configurations,
 * connection management, metadata discovery, and validation logic required by the pipeline to
 * integrate with the target database.
 */
public interface ISpToSrcSourceConnector {

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
   * Parses the shard configuration file to a list of shards.
   *
   * @param shardFilePath The GCS path to the shard configuration file.
   * @return A list of Shards.
   * @throws Exception If parsing fails.
   */
  List<Shard> parseShardConfig(String shardFilePath) throws Exception;

  /**
   * Validates the shards and pipeline options.
   *
   * @param shards The list of shards to validate.
   * @param options The pipeline options.
   * @throws Exception If validation fails.
   */
  void validate(List<Shard> shards, PipelineOptions options) throws Exception;

  /**
   * Scans the source/destination database to retrieve its information schema.
   *
   * @param shards The list of shards.
   * @return The SourceSchema.
   * @throws Exception If scanning fails.
   */
  SourceSchema getInformationSchema(List<Shard> shards) throws Exception;

  /**
   * Returns whether this source supports sharded migrations.
   *
   * @return True if supports sharding, false otherwise.
   */
  boolean supportsSharding();

  /**
   * Returns whether the pipeline should update the Spanner record with values read from Spanner
   * during stale reads (e.g. for DELETE events). //TODO this flag is only required by cassandra. We
   * should look for a way to remove it
   *
   * @return True if it should update, false otherwise.
   */
  boolean shouldUpdateReadValuesToSpannerRecord();

  /**
   * Classifies a dialect-specific exception into retryable or permanent.
   *
   * @param cause The cause to classify.
   * @return The TupleTag (Constants.PERMANENT_ERROR_TAG or Constants.RETRYABLE_ERROR_TAG) if
   *     classified, or null to fallback to general classification.
   */
  org.apache.beam.sdk.values.TupleTag<String> classifyException(Throwable cause);
}
