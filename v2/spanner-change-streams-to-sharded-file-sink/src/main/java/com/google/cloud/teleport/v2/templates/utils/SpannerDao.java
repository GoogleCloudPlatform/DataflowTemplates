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
package com.google.cloud.teleport.v2.templates.utils;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;

/** Handles Spanner interaction. */
public class SpannerDao {

  private SpannerAccessor spannerAccessor;
  private SpannerConfig spannerConfig;
  // Timeout for Cloud Spanner schema update.
  private static final int SCHEMA_UPDATE_WAIT_MIN = 5;

  public SpannerDao(String projectId, String instanceId, String databaseId) {
    this.spannerConfig =
        SpannerConfig.create()
            .withProjectId(projectId)
            .withInstanceId(instanceId)
            .withDatabaseId(databaseId);
    this.spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    // TODO: add support for Spanner Postgres dialect
  }

  /*
  This method creates the table to capture start and duration.
  */
  private void checkAndCreateMetadataTable() {

    DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();
    String statement =
        "select * from information_schema.tables where table_name='spanner_to_gcs_metadata' and"
            + " table_type='BASE TABLE'";
    try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction()) {
      ResultSet resultSet = tx.executeQuery(Statement.of(statement));
      if (!resultSet.next()) {
        DatabaseAdminClient databaseAdminClient = spannerAccessor.getDatabaseAdminClient();
        String createTable =
            "CREATE TABLE spanner_to_gcs_metadata "
                + " (start_time STRING(MAX) NOT NULL,duration STRING(MAX) NOT NULL) PRIMARY KEY()";
        OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
            databaseAdminClient.updateDatabaseDdl(
                spannerConfig.getInstanceId().get(),
                spannerConfig.getDatabaseId().get(),
                Arrays.asList(createTable),
                null);

        try {
          op.get(SCHEMA_UPDATE_WAIT_MIN, TimeUnit.MINUTES);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          throw new RuntimeException(e);
        }
      }
    } catch (Exception e) {

      throw new RuntimeException(e);
    }
  }

  private void checkAndCreateProgressTable() {

    DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();
    String statement =
        "select * from information_schema.tables where table_name='shard_file_create_progress' and"
            + " table_type='BASE TABLE'";
    try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction()) {
      ResultSet resultSet = tx.executeQuery(Statement.of(statement));
      if (!resultSet.next()) {
        DatabaseAdminClient databaseAdminClient = spannerAccessor.getDatabaseAdminClient();
        String createTable =
            "CREATE TABLE shard_file_create_progress (shard STRING(MAX) NOT NULL, "
                + " created_upto TIMESTAMP NOT NULL ) PRIMARY KEY(shard) ";
        OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
            databaseAdminClient.updateDatabaseDdl(
                spannerConfig.getInstanceId().get(),
                spannerConfig.getDatabaseId().get(),
                Arrays.asList(createTable),
                null);

        try {
          op.get(SCHEMA_UPDATE_WAIT_MIN, TimeUnit.MINUTES);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          throw new RuntimeException(e);
        }
      }
    } catch (Exception e) {

      throw new RuntimeException(e);
    }
  }

  public void initShardProgress(List<Shard> shards) {
    checkAndCreateProgressTable();
    List<Mutation> mutations = new ArrayList<>();
    Timestamp epochTimestamp = Timestamp.parseTimestamp("1970-01-01T12:00:00Z");
    for (Shard shard : shards) {

      mutations.add(
          Mutation.newInsertOrUpdateBuilder("shard_file_create_progress")
              .set("shard")
              .to(shard.getLogicalShardId())
              .set("created_upto")
              .to(epochTimestamp)
              .build());
    }
    spannerAccessor
        .getDatabaseClient()
        .readWriteTransaction()
        .run(
            (TransactionCallable<Void>)
                transaction -> {
                  transaction.buffer(mutations);
                  return null;
                });
    close();
  }

  /**
   * Writes the job's start time and window duration to the spanner_to_gcs_metadata table. The table
   * only has one record, so the mutation will always override the record.
   */
  public void writeStartAndDuration(String start, String duration) {

    // create the tables needed for the pipeline
    checkAndCreateMetadataTable();
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertOrUpdateBuilder("spanner_to_gcs_metadata")
            .set("start_time")
            .to(start)
            .set("duration")
            .to(duration)
            .build());
    spannerAccessor.getDatabaseClient().write(mutations);
    close();
  }

  public void updateProgress(String shard, String endTime) {

    Timestamp endTimestamp = Timestamp.parseTimestamp(endTime);
    DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();

    String updateStatementStr =
        "update shard_file_create_progress set created_upto=@endTimestamp "
            + " where shard=@shardId and created_upto<@endTimestamp";
    Statement updateStatement =
        Statement.newBuilder(updateStatementStr)
            .bind("endTimestamp")
            .to(endTimestamp)
            .bind("shardId")
            .to(shard)
            .build();

    long updateCount =
        databaseClient
            .readWriteTransaction()
            .run(
                transaction -> {
                  return transaction.executeUpdate(updateStatement);
                });
  }

  public void close() {
    spannerAccessor.close();
  }
}
