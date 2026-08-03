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
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.TransactionalCheck;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.SpannerMutationResponse;
import com.google.common.collect.ImmutableList;
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

  public SpannerTargetDao(
      String connectionKey, IConnectionHelper<DatabaseClient> connectionHelper) {
    this.connectionKey = connectionKey;
    this.connectionHelper = connectionHelper;
  }

  @Override
  public void write(
      DMLGeneratorResponse dmlGeneratorResponse, TransactionalCheck transactionalCheck)
      throws Exception {
    if (transactionalCheck != null) {
      throw new UnsupportedOperationException(
          "TransactionalCheck is not supported for the Spanner target DAO.");
    }

    if (!(dmlGeneratorResponse instanceof SpannerMutationResponse)) {
      throw new IllegalArgumentException(
          "Expected SpannerMutationResponse but received: "
              + dmlGeneratorResponse.getClass().getSimpleName());
    }

    DatabaseClient client = connectionHelper.getConnection(connectionKey);
    if (client == null) {
      throw new ConnectionException("DatabaseClient is null for connection key: " + connectionKey);
    }

    Mutation mutation = ((SpannerMutationResponse) dmlGeneratorResponse).getMutation();
    client.writeAtLeastOnce(ImmutableList.of(mutation));
    LOG.debug("Successfully wrote mutation via SpannerTargetDao for key: {}", connectionKey);
  }

  @Override
  public void write(
      DMLGeneratorResponse dmlGeneratorResponse,
      TransactionalCheck transactionalCheck,
      Object transactionContext)
      throws Exception {
    if (transactionContext == null) {
      write(dmlGeneratorResponse, transactionalCheck);
      return;
    }

    if (transactionalCheck != null) {
      throw new UnsupportedOperationException(
          "TransactionalCheck is not supported for the Spanner target DAO.");
    }

    if (!(dmlGeneratorResponse instanceof SpannerMutationResponse)) {
      throw new IllegalArgumentException(
          "Expected SpannerMutationResponse but received: "
              + dmlGeneratorResponse.getClass().getSimpleName());
    }

    Mutation mutation = ((SpannerMutationResponse) dmlGeneratorResponse).getMutation();
    ((com.google.cloud.spanner.TransactionContext) transactionContext).buffer(mutation);
    LOG.debug("Successfully buffered mutation via SpannerTargetDao for key: {}", connectionKey);
  }
}
