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
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.SpannerMutationResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerTargetDaoTest {

  private static final String CONNECTION_KEY = "my-project/my-instance/my-db";

  @Test
  @SuppressWarnings("unchecked")
  public void writeDispatchesMutationToDatabaseClient() throws Exception {
    IConnectionHelper<DatabaseClient> connectionHelper = mock(IConnectionHelper.class);
    DatabaseClient mockClient = mock(DatabaseClient.class);
    when(connectionHelper.getConnection(CONNECTION_KEY)).thenReturn(mockClient);

    Mutation mutation = Mutation.newInsertOrUpdateBuilder("T").set("Id").to(1L).build();
    SpannerMutationResponse response = new SpannerMutationResponse(mutation);

    SpannerTargetDao dao = new SpannerTargetDao(CONNECTION_KEY, connectionHelper);
    dao.write(response, null);

    verify(mockClient).writeAtLeastOnce(anyIterable());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void transactionalCheckNotSupportedThrows() {
    IConnectionHelper<DatabaseClient> connectionHelper = mock(IConnectionHelper.class);
    SpannerTargetDao dao = new SpannerTargetDao(CONNECTION_KEY, connectionHelper);

    Mutation mutation = Mutation.newInsertOrUpdateBuilder("T").set("Id").to(1L).build();
    SpannerMutationResponse response = new SpannerMutationResponse(mutation);

    assertThrows(UnsupportedOperationException.class, () -> dao.write(response, () -> {}));
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

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteBuffersMutationWhenTransactionContextProvided() throws Exception {
    IConnectionHelper<DatabaseClient> connectionHelper = mock(IConnectionHelper.class);
    com.google.cloud.spanner.TransactionContext mockTxnContext =
        mock(com.google.cloud.spanner.TransactionContext.class);

    Mutation mutation = Mutation.newInsertOrUpdateBuilder("T").set("Id").to(1L).build();
    SpannerMutationResponse response = new SpannerMutationResponse(mutation);

    SpannerTargetDao dao = new SpannerTargetDao(CONNECTION_KEY, connectionHelper);
    dao.write(response, null, mockTxnContext);

    verify(mockTxnContext).buffer(mutation);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteThrowsOnTransactionalCheck() {
    IConnectionHelper<DatabaseClient> connectionHelper = mock(IConnectionHelper.class);
    SpannerTargetDao dao = new SpannerTargetDao(CONNECTION_KEY, connectionHelper);
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            dao.write(
                new SpannerMutationResponse(null),
                mock(
                    com.google.cloud.teleport.v2.templates.dbutils.dao.source.TransactionalCheck
                        .class)));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            dao.write(
                new SpannerMutationResponse(null),
                mock(
                    com.google.cloud.teleport.v2.templates.dbutils.dao.source.TransactionalCheck
                        .class),
                new Object()));
  }
}
