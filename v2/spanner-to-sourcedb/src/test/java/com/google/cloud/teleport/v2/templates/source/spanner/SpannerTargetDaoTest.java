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

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.SpannerMutationResponse;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerTargetDaoTest {

  private static final String CONNECTION_KEY = "my-project/my-instance/my-db";

  private static Ddl buildTestDdl() {
    Ddl.Builder builder = Ddl.builder();
    Table.Builder tableBuilder = builder.createTable("T");
    tableBuilder.column("Id").int64().notNull().endColumn();
    tableBuilder.primaryKey().asc("Id").end();
    tableBuilder.endTable();
    return builder.build();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void writeDispatchesMutationToDatabaseClient() throws Exception {
    IConnectionHelper<DatabaseClient> connectionHelper = mock(IConnectionHelper.class);
    DatabaseClient mockClient = mock(DatabaseClient.class);
    TransactionRunner mockRunner = mock(TransactionRunner.class);
    TransactionContext mockTxnContext = mock(TransactionContext.class);

    when(connectionHelper.getConnection(CONNECTION_KEY)).thenReturn(mockClient);
    when(mockClient.readWriteTransaction(any())).thenReturn(mockRunner);
    when(mockRunner.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(mockTxnContext);
            });

    Mutation mutation = Mutation.newInsertOrUpdateBuilder("T").set("Id").to(1L).build();
    SpannerMutationResponse response = new SpannerMutationResponse(mutation);

    SpannerTargetDao dao = new SpannerTargetDao(CONNECTION_KEY, connectionHelper);
    dao.write(response, null);

    verify(mockTxnContext).buffer(ImmutableList.of(mutation));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void writeExecutesExclusiveLockReadWhenDdlAndKeyPresent() throws Exception {
    IConnectionHelper<DatabaseClient> connectionHelper = mock(IConnectionHelper.class);
    DatabaseClient mockClient = mock(DatabaseClient.class);
    TransactionRunner mockRunner = mock(TransactionRunner.class);
    TransactionContext mockTxnContext = mock(TransactionContext.class);
    ResultSet mockResultSet = mock(ResultSet.class);

    when(connectionHelper.getConnection(CONNECTION_KEY)).thenReturn(mockClient);
    when(mockClient.readWriteTransaction(any())).thenReturn(mockRunner);
    when(mockRunner.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(mockTxnContext);
            });
    when(mockTxnContext.executeQuery(any(Statement.class))).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);

    Ddl ddl = buildTestDdl();
    Key key = Key.of(1L);
    Mutation mutation = Mutation.newInsertOrUpdateBuilder("T").set("Id").to(1L).build();
    SpannerMutationResponse response = new SpannerMutationResponse(mutation, key);

    SpannerTargetDao dao = new SpannerTargetDao(CONNECTION_KEY, connectionHelper, ddl);
    dao.write(response, null);

    verify(mockTxnContext).executeQuery(any(Statement.class));
    verify(mockResultSet).getCurrentRowAsStruct();
    verify(mockTxnContext).buffer(ImmutableList.of(mutation));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void transactionalCheckIsExecuted() throws Exception {
    IConnectionHelper<DatabaseClient> connectionHelper = mock(IConnectionHelper.class);
    DatabaseClient mockClient = mock(DatabaseClient.class);
    TransactionRunner mockRunner = mock(TransactionRunner.class);
    TransactionContext mockTxnContext = mock(TransactionContext.class);

    when(connectionHelper.getConnection(CONNECTION_KEY)).thenReturn(mockClient);
    when(mockClient.readWriteTransaction(any())).thenReturn(mockRunner);
    when(mockRunner.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(mockTxnContext);
            });

    Mutation mutation = Mutation.newInsertOrUpdateBuilder("T").set("Id").to(1L).build();
    SpannerMutationResponse response = new SpannerMutationResponse(mutation);

    com.google.cloud.teleport.v2.templates.dbutils.dao.source.TransactionalCheck mockCheck =
        mock(com.google.cloud.teleport.v2.templates.dbutils.dao.source.TransactionalCheck.class);

    SpannerTargetDao dao = new SpannerTargetDao(CONNECTION_KEY, connectionHelper);
    dao.write(response, mockCheck);

    verify(mockCheck).check();
    verify(mockTxnContext).buffer(ImmutableList.of(mutation));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void wrongResponseTypeThrows() throws Exception {
    IConnectionHelper<DatabaseClient> connectionHelper = mock(IConnectionHelper.class);
    DatabaseClient mockClient = mock(DatabaseClient.class);
    when(connectionHelper.getConnection(CONNECTION_KEY)).thenReturn(mockClient);

    DMLGeneratorResponse wrongResponse = new DMLGeneratorResponse("SELECT 1");

    SpannerTargetDao dao = new SpannerTargetDao(CONNECTION_KEY, connectionHelper);
    assertThrows(IllegalArgumentException.class, () -> dao.write(wrongResponse, null));
  }
}
