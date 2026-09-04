/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates.source.sqlserver;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.services.datastream.v1.model.SourceConfig;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequence;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequenceCreationException;
import com.google.cloud.teleport.v2.templates.source.IDsToSpSourceConnector;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

/** SqlServer implementation of {@link IDsToSpSourceConnector} connector. */
public class SqlServerDsToSpSourceConnector implements IDsToSpSourceConnector {

  /* List of Event keys, Shadow table information related to sort order in SqlServer database. */
  public static final String SQLSERVER_TIMESTAMP_KEY = "_metadata_timestamp";
  public static final Pair<String, String> SQLSERVER_TIMESTAMP_SHADOW_INFO =
      Pair.of("timestamp", "INT64");
  public static final Pair<String, String> SQLSERVER_TIMESTAMP_SHADOW_INFO_PG_DIALECT =
      Pair.of("timestamp", "bigint");
  public static final String SQLSERVER_CHANGE_LSN_KEY = "_metadata_change_lsn";
  public static final Pair<String, String> SQLSERVER_CHANGE_LSN_SHADOW_INFO =
      Pair.of("change_lsn", "STRING(MAX)");
  public static final Pair<String, String> SQLSERVER_CHANGE_LSN_SHADOW_INFO_PG_DIALECT =
      Pair.of("change_lsn", "character varying(2621440)");
  /* Mapping from Event keys to shadow table information for SqlServer database with gsql dialect*/
  public static final Map<String, Pair<String, String>> SQLSERVER_SORT_ORDER =
      ImmutableMap.of(
          SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY,
          SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_SHADOW_INFO,
          SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY,
          SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_SHADOW_INFO);
  /* Mapping from Event keys to shadow table information for SqlServer database with postgres dialect*/
  public static final Map<String, Pair<String, String>> SQLSERVER_SORT_ORDER_PG_DIALECT =
      ImmutableMap.of(
          SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY,
          SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_SHADOW_INFO_PG_DIALECT,
          SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY,
          SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_SHADOW_INFO_PG_DIALECT);

  @Override
  public String getSourceType() {
    return "sqlserver";
  }

  @Override
  public boolean matchesSourceConfig(SourceConfig sourceConfig) {
    return sourceConfig.getSqlServerSourceConfig() != null;
  }

  @Override
  public Map<String, Pair<String, String>> getSortOrder(Dialect dialect) {
    if (dialect == Dialect.POSTGRESQL) {
      return SQLSERVER_SORT_ORDER_PG_DIALECT;
    }
    return SQLSERVER_SORT_ORDER;
  }

  @Override
  public ChangeEventContext createChangeEventContext(
      JsonNode changeEvent, Ddl ddl, Ddl shadowTableDdl, String shadowTablePrefix)
      throws ChangeEventConvertorException, InvalidChangeEventException, DroppedTableException {
    return new SqlServerChangeEventContext(changeEvent, ddl, shadowTableDdl, shadowTablePrefix);
  }

  @Override
  public ChangeEventSequence createChangeEventSequenceFromChangeEventContext(
      ChangeEventContext changeEventContext)
      throws ChangeEventConvertorException, InvalidChangeEventException {
    return SqlServerChangeEventSequence.createFromChangeEvent(changeEventContext);
  }

  @Override
  public ChangeEventSequence createChangeEventSequenceFromShadowTable(
      TransactionContext transactionContext,
      ChangeEventContext changeEventContext,
      Ddl shadowDdl,
      boolean useSqlStatements)
      throws ChangeEventSequenceCreationException, InvalidChangeEventException {
    return SqlServerChangeEventSequence.createFromShadowTable(
        transactionContext, changeEventContext, shadowDdl, useSqlStatements);
  }
}
