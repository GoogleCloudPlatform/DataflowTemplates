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
import com.google.cloud.teleport.v2.spanner.migrations.metadata.SpannerToGcsJobMetadata;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles Spanner interaction. */
public class SpannerDao {

  private SpannerAccessor spannerAccessor;
  private SpannerConfig spannerConfig;
  private String
      shardFileCreateProgressTableName; // stores latest window of files created per shard
  private String
      spannerToGcsMetadataTableName; // stores the start time and duration supplied in input
  private String
      shardFileProcessProgressTableName; // stores the window until which shard has progressed
  private String dataSeenTableName; // stores the window end for change data records seen
  private boolean isPostgres;
  // Timeout for Cloud Spanner schema update.
  private static final int SCHEMA_UPDATE_WAIT_MIN = 5;
  private static final Logger LOG = LoggerFactory.getLogger(SpannerDao.class);

  public SpannerDao(SpannerConfig spannerConfig, String tableSuffix, boolean isPostgres) {
    this.spannerConfig = spannerConfig;
    this.spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    this.isPostgres = isPostgres;
    createInternal(tableSuffix);
  }

  public SpannerDao(
      String projectId,
      String instanceId,
      String databaseId,
      String tableSuffix,
      boolean isPostgres) {
    this.spannerConfig =
        SpannerConfig.create()
            .withProjectId(projectId)
            .withInstanceId(instanceId)
            .withDatabaseId(databaseId);
    this.spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    this.isPostgres = isPostgres;

    createInternal(tableSuffix);
  }

  private void createInternal(String tableSuffix) {
    spannerToGcsMetadataTableName = "spanner_to_gcs_metadata";
    shardFileCreateProgressTableName = "shard_file_create_progress";
    shardFileProcessProgressTableName = "shard_file_process_progress";
    dataSeenTableName = "data_seen";
    if (!tableSuffix.isEmpty()) {
      spannerToGcsMetadataTableName += "_" + tableSuffix;
      shardFileCreateProgressTableName += "_" + tableSuffix;
      dataSeenTableName += "_" + tableSuffix;
      shardFileProcessProgressTableName += "_" + tableSuffix;
    }
  }

  /*
  This method creates the table to capture start and duration.
  */
  private void checkAndCreateMetadataTable() {

    DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();

    String statement =
        "select * from information_schema.tables where table_name='"
            + spannerToGcsMetadataTableName
            + "' and"
            + " table_type='BASE TABLE'";
    try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction()) {
      ResultSet resultSet = tx.executeQuery(Statement.of(statement));
      if (!resultSet.next()) {
        DatabaseAdminClient databaseAdminClient = spannerAccessor.getDatabaseAdminClient();
        String createTable = "";
        if (isPostgres) {
          createTable =
              "CREATE TABLE "
                  + spannerToGcsMetadataTableName
                  + " ( run_id character varying NOT NULL,start_time character varying NOT NULL,"
                  + " duration character varying NOT NULL , PRIMARY KEY(run_id)) ";
        } else {
          createTable =
              "CREATE TABLE "
                  + spannerToGcsMetadataTableName
                  + " (run_id STRING(MAX) NOT NULL,start_time STRING(MAX) NOT NULL,duration"
                  + " STRING(MAX) NOT NULL) PRIMARY KEY(run_id)";
        }
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
        "select * from information_schema.tables where table_name='"
            + shardFileCreateProgressTableName
            + "' and"
            + " table_type='BASE TABLE'";
    try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction()) {
      ResultSet resultSet = tx.executeQuery(Statement.of(statement));
      if (!resultSet.next()) {
        DatabaseAdminClient databaseAdminClient = spannerAccessor.getDatabaseAdminClient();
        String createTable = "";
        if (isPostgres) {
          createTable =
              "CREATE TABLE "
                  + shardFileCreateProgressTableName
                  + "( run_id character varying NOT NULL,shard character varying NOT"
                  + " NULL,created_upto timestamp with time zone NOT NULL,PRIMARY"
                  + " KEY(run_id,shard))";

        } else {
          createTable =
              "CREATE TABLE "
                  + shardFileCreateProgressTableName
                  + " (run_id STRING(MAX) NOT NULL,shard STRING(MAX) NOT NULL, "
                  + " created_upto TIMESTAMP NOT NULL ) PRIMARY KEY(run_id,shard) ";
        }
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

  private void checkAndCreateDataSeenTable() {

    DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();

    String statement =
        "select * from information_schema.tables where table_name='"
            + dataSeenTableName
            + "' and"
            + " table_type='BASE TABLE'";
    try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction()) {
      ResultSet resultSet = tx.executeQuery(Statement.of(statement));
      if (!resultSet.next()) {
        DatabaseAdminClient databaseAdminClient = spannerAccessor.getDatabaseAdminClient();
        String createTable = "";
        if (isPostgres) {
          createTable =
              "CREATE TABLE "
                  + dataSeenTableName
                  + "( id character varying NOT NULL, "
                  + " run_id character varying NOT NULL,shard character"
                  + " varying NOT NULL,window_seen timestamp with time zone NOT NULL,update_ts"
                  + " timestamp with time zone DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(id))"
                  + " TTL INTERVAL '2 days' ON update_ts";

        } else {
          createTable =
              "CREATE TABLE "
                  + dataSeenTableName
                  + " (id STRING(MAX) NOT NULL, run_id"
                  + " STRING(MAX) NOT NULL,shard STRING(MAX) NOT NULL, window_seen TIMESTAMP NOT"
                  + " NULL , update_ts TIMESTAMP DEFAULT (CURRENT_TIMESTAMP)) PRIMARY"
                  + " KEY(id) , ROW DELETION POLICY (OLDER_THAN(update_ts, INTERVAL 2 DAY))";
        }
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

  public void initShardProgress(List<Shard> shards, String runId) {
    checkAndCreateDataSeenTable();
    checkAndCreateProgressTable();
    List<Mutation> mutations = new ArrayList<>();
    Timestamp epochTimestamp = Timestamp.parseTimestamp("1970-01-01T12:00:00Z");
    for (Shard shard : shards) {

      mutations.add(
          Mutation.newInsertOrUpdateBuilder(shardFileCreateProgressTableName)
              .set("run_id")
              .to(runId)
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
  }

  /**
   * Writes the job's start time and window duration to the spanner_to_gcs_metadata table. The table
   * has one record per run.
   */
  public void upsertSpannerToGcsMetadata(String start, String duration, String runId) {

    // create the tables needed for the pipeline
    checkAndCreateMetadataTable();
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertOrUpdateBuilder(spannerToGcsMetadataTableName)
            .set("run_id")
            .to(runId)
            .set("start_time")
            .to(start)
            .set("duration")
            .to(duration)
            .build());
    spannerAccessor.getDatabaseClient().write(mutations);
  }

  public void updateDataSeen(String id, String shard, Timestamp endTimestamp, String runId) {

    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertBuilder(dataSeenTableName)
            .set("id")
            .to(id)
            .set("run_id")
            .to(runId)
            .set("shard")
            .to(shard)
            .set("window_seen")
            .to(endTimestamp)
            .build());
    spannerAccessor.getDatabaseClient().write(mutations);
  }

  public void updateProgress(String shard, String endTime, String runId) {

    Timestamp endTimestamp = Timestamp.parseTimestamp(endTime);
    DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();

    Statement updateStatement;
    if (isPostgres) {
      String updateStatementStr =
          "update "
              + shardFileCreateProgressTableName
              + " set created_upto=$1 "
              + " where run_id=$2 and shard=$3 and created_upto<$4 ";
      updateStatement =
          Statement.newBuilder(updateStatementStr)
              .bind("p1")
              .to(endTimestamp)
              .bind("p2")
              .to(runId)
              .bind("p3")
              .to(shard)
              .bind("p4")
              .to(endTimestamp)
              .build();
    } else {
      String updateStatementStr =
          "update "
              + shardFileCreateProgressTableName
              + " set created_upto=@endTimestamp "
              + " where run_id=@runId and shard=@shardId and created_upto<@endTimestamp";
      updateStatement =
          Statement.newBuilder(updateStatementStr)
              .bind("runId")
              .to(runId)
              .bind("endTimestamp")
              .to(endTimestamp)
              .bind("shardId")
              .to(shard)
              .build();
    }
    long updateCount =
        databaseClient
            .readWriteTransaction()
            .run(
                transaction -> {
                  return transaction.executeUpdate(updateStatement);
                });
  }

  public Timestamp getMinimumStartTimeAcrossAllShards(String runId) {
    DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();
    Statement statement;
    Timestamp start = null;
    if (isPostgres) {
      String statementStr =
          "SELECT min(file_start_interval) from "
              + shardFileProcessProgressTableName
              + " where run_id=$1";
      statement = Statement.newBuilder(statementStr).bind("p1").to(runId).build();
    } else {
      String statementStr =
          "SELECT min(file_start_interval) from "
              + shardFileProcessProgressTableName
              + " where run_id=@runId";
      statement = Statement.newBuilder(statementStr).bind("runId").to(runId).build();
    }
    try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction()) {

      ResultSet resultSet = tx.executeQuery(statement);
      if (resultSet.next()) {
        start = resultSet.getTimestamp(0);
      }
    } catch (Exception e) {
      // When there are no records for a given run_id, the exception thrown has text contains NULL
      // value
      if (e.getMessage().contains("Table not found")
          || e.getMessage().contains("contains NULL value")) {
        return null;
      } else {
        throw new RuntimeException(
            "The " + shardFileProcessProgressTableName + " table could not be read. ", e);
      }
    }
    return start;
  }

  public SpannerToGcsJobMetadata getSpannerToGcsJobMetadata(String runId) {
    DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();
    Statement statement;
    if (isPostgres) {
      String statementStr =
          "SELECT start_time, duration from "
              + spannerToGcsMetadataTableName
              + " where run_id = $1";
      statement = Statement.newBuilder(statementStr).bind("p1").to(runId).build();
    } else {
      String statementStr =
          "SELECT start_time, duration from "
              + spannerToGcsMetadataTableName
              + " where run_id = @runId";
      statement = Statement.newBuilder(statementStr).bind("runId").to(runId).build();
    }

    try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction()) {
      ResultSet resultSet = tx.executeQuery(statement);

      if (resultSet.next()) {
        String startTime = resultSet.getString(0);
        String duration = resultSet.getString(1);

        SpannerToGcsJobMetadata rec = new SpannerToGcsJobMetadata(startTime, duration);
        return rec;
      }
    } catch (Exception e) {
      if (e.getMessage().contains("Table not found")) {
        return null;
      } else {
        throw new RuntimeException(
            "The " + spannerToGcsMetadataTableName + " table could not be read. ", e);
      }
    }

    return null;
  }

  public boolean doesIdExist(String id) {

    DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();
    Statement statement;
    if (isPostgres) {
      String statementStr = "SELECT window_seen from " + dataSeenTableName + " where id=$1";
      statement = Statement.newBuilder(statementStr).bind("p1").to(id).build();

    } else {
      String statementStr = "SELECT window_seen from " + dataSeenTableName + " where id=@id";
      statement = Statement.newBuilder(statementStr).bind("id").to(id).build();
    }

    try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction()) {
      ResultSet resultSet = tx.executeQuery(statement);

      if (resultSet.next()) {
        return true;
      }
    } catch (Exception e) {
      LOG.info("The " + dataSeenTableName + " table could not be read. ");
      // We need to throw the original exception such that the caller can
      // look at SpannerException class to take decision
      throw e;
    }

    return false;
  }

  public void close() {
    spannerAccessor.close();
  }
}
