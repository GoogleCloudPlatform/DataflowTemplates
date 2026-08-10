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
package com.google.cloud.teleport.v2.templates.source.spanner;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.spanner.migrations.spanner.SpannerReadUtils;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.TransactionalCheck;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.SpannerMutationResponse;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DAO for writing reverse-replicated records to a target Cloud Spanner database.
 *
 * <p>Receives a {@link SpannerMutationResponse} from the DML generator and commits the contained
 * {@link Mutation} via a {@link DatabaseClient} obtained from the {@link
 * com.google.cloud.teleport.v2.templates.dbutils.connection.SpannerConnectionHelper}.
 */
public class SpannerTargetDao implements IDao {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerTargetDao.class);

  private final String connectionKey;
  private final IConnectionHelper<DatabaseClient> connectionHelper;
  private final Ddl targetDdl;

  public SpannerTargetDao(
      String connectionKey, IConnectionHelper<DatabaseClient> connectionHelper, Ddl targetDdl) {
    this.connectionKey = connectionKey;
    this.connectionHelper = connectionHelper;
    this.targetDdl = targetDdl;
  }

  public SpannerTargetDao(
      String connectionKey, IConnectionHelper<DatabaseClient> connectionHelper) {
    this(connectionKey, connectionHelper, null);
  }

  @Override
  public void write(
      DMLGeneratorResponse dmlGeneratorResponse, TransactionalCheck transactionalCheck)
      throws Exception {
    if (!(dmlGeneratorResponse instanceof SpannerMutationResponse spannerMutationResponse)) {
      throw new IllegalArgumentException(
          "Expected SpannerMutationResponse but received: "
              + dmlGeneratorResponse.getClass().getSimpleName());
    }

    DatabaseClient client = connectionHelper.getConnection(connectionKey);
    if (client == null) {
      throw new ConnectionException("DatabaseClient is null for connection key: " + connectionKey);
    }

    Mutation mutation = spannerMutationResponse.getMutation();
    Key primaryKey = spannerMutationResponse.getPrimaryKey();

    //TODO - optimize this flow to avoid the nested transaction if the shadow transaction
    // (in the transaction check) and the main transaction are on the same database
    client
        .readWriteTransaction(Options.priority(RpcPriority.HIGH))
        .run(
            (TransactionRunner.TransactionCallable<Void>)
                mainTxn -> {
                  if (targetDdl != null && primaryKey != null) {
                    readDataTableRowWithExclusiveLock(
                        mainTxn, mutation.getTable(), primaryKey, targetDdl);
                  }
                  if (transactionalCheck != null) {
                    transactionalCheck.check();
                  }
                  //TODO- add support for delete where PK has changed - similar to data dml in live flow
                  mainTxn.buffer(ImmutableList.of(mutation));
                  return null;
                });
  }

  private void readDataTableRowWithExclusiveLock(
      TransactionContext transactionContext, String tableName, Key primaryKey, Ddl ddl) {
    Table table = ddl.table(tableName);
    if (table == null) {
      LOG.warn("Table '{}' not found in target DDL; skipping exclusive lock read.", tableName);
      return;
    }
    List<String> columnNames =
        table.primaryKeys().stream().map(IndexColumn::name).collect(Collectors.toList());
    Statement sql =
        SpannerReadUtils.generateReadSQLWithExclusiveLock(tableName, columnNames, primaryKey, ddl);
    ResultSet resultSet = transactionContext.executeQuery(sql);
    if (!resultSet.next()) {
      return;
    }
    resultSet.getCurrentRowAsStruct();
  }
}
