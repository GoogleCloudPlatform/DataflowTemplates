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
package com.google.cloud.teleport.v2.templates.dao;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.teleport.v2.templates.common.ShardProgress;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;

/** Read and writes the shard process to shard_progress table. */
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
  }

  public Map<String, ShardProgress> getShardProgress() {
    Map<String, ShardProgress> shardProgress = new HashMap<>();
    DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();
    String statement = "SELECT shard, start, status from shard_progress where status='REPROCESS";
    try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction()) {
      ResultSet resultSet = tx.executeQuery(Statement.of(statement));

      while (resultSet.next()) {
        String shard = resultSet.getString(0);
        Timestamp start = resultSet.getTimestamp(1);
        String status = resultSet.getString(2);
        ShardProgress rec = new ShardProgress(shard, start, status);
        shardProgress.put(shard, rec);
      }
    } catch (Exception e) {

      throw new RuntimeException("The shard_progress table could not be read. " + e.getMessage());
    }
    return shardProgress;
  }

  public void writeShardProgress(ShardProgress shardProgress) {
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertOrUpdateBuilder("shard_progress")
            .set("shard")
            .to(shardProgress.getShard())
            .set("start")
            .to(shardProgress.getStart())
            .set("status")
            .to(shardProgress.getStatus())
            .build());
    spannerAccessor.getDatabaseClient().write(mutations);
  }

  /*
  This method creates the table to track the shard progress.
  It stores the start time of the file that have been successfully processed or
  that has encountered error.
  */
  public void checkAndcreateShardProgressTable() {

    DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();
    String statement =
        "select * from information_schema.tables where table_name='shard_progress' and"
            + " table_type='BASE TABLE'";
    try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction()) {
      ResultSet resultSet = tx.executeQuery(Statement.of(statement));
      if (!resultSet.next()) {
        DatabaseAdminClient databaseAdminClient = spannerAccessor.getDatabaseAdminClient();
        String createTable =
            "create table shard_progress (shard STRING(MAX) NOT NULL,start TIMESTAMP NOT"
                + " NULL,status STRING(MAX) NOT NULL,) PRIMARY KEY(shard)";
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

  public void close() {
    spannerAccessor.close();
  }
}
