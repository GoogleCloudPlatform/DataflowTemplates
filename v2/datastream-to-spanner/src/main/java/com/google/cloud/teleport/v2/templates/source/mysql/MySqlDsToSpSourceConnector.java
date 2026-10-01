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
package com.google.cloud.teleport.v2.templates.source.mysql;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.services.datastream.v1.model.SourceConfig;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import com.google.cloud.teleport.v2.spanner.source.SourceConstants;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequence;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequenceCreationException;
import com.google.cloud.teleport.v2.templates.source.IDsToSpSourceConnector;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

/** MySQL implementation of {@link IDsToSpSourceConnector} connector. */
public class MySqlDsToSpSourceConnector implements IDsToSpSourceConnector {

  /* List of Event keys, Shadow table information related to sort order in MySQL database. */
  public static final String MYSQL_TIMESTAMP_KEY = "_metadata_timestamp";
  public static final Pair<String, String> MYSQL_TIMESTAMP_SHADOW_INFO =
      Pair.of("timestamp", "INT64");
  public static final Pair<String, String> MYSQL_TIMESTAMP_SHADOW_INFO_PG_DIALECT =
      Pair.of("timestamp", "bigint");
  public static final String MYSQL_LOGFILE_KEY = "_metadata_log_file";
  public static final Pair<String, String> MYSQL_LOGFILE_SHADOW_INFO =
      Pair.of("log_file", "STRING(MAX)");
  public static final Pair<String, String> MYSQL_LOGFILE_SHADOW_INFO_PG_DIALECT =
      Pair.of("log_file", "character varying(2621440)");
  public static final String MYSQL_LOGPOSITION_KEY = "_metadata_log_position";
  public static final Pair<String, String> MYSQL_LOGPOSITION_SHADOW_INFO =
      Pair.of("log_position", "INT64");
  public static final Pair<String, String> MYSQL_LOGPOSITION_SHADOW_INFO_PG_DIALECT =
      Pair.of("log_position", "bigint");
  /* Mapping from Event keys to shadow table information for MySql database with gsql dialect*/
  public static final Map<String, Pair<String, String>> MYSQL_SORT_ORDER =
      ImmutableMap.of(
          MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY,
          MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_SHADOW_INFO,
          MySqlDsToSpSourceConnector.MYSQL_LOGFILE_KEY,
          MySqlDsToSpSourceConnector.MYSQL_LOGFILE_SHADOW_INFO,
          MySqlDsToSpSourceConnector.MYSQL_LOGPOSITION_KEY,
          MySqlDsToSpSourceConnector.MYSQL_LOGPOSITION_SHADOW_INFO);
  /* Mapping from Event keys to shadow table information for MySql database with postgres dialect*/
  public static final Map<String, Pair<String, String>> MYSQL_SORT_ORDER_PG_DIALECT =
      ImmutableMap.of(
          MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY,
          MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_SHADOW_INFO_PG_DIALECT,
          MySqlDsToSpSourceConnector.MYSQL_LOGFILE_KEY,
          MySqlDsToSpSourceConnector.MYSQL_LOGFILE_SHADOW_INFO_PG_DIALECT,
          MySqlDsToSpSourceConnector.MYSQL_LOGPOSITION_KEY,
          MySqlDsToSpSourceConnector.MYSQL_LOGPOSITION_SHADOW_INFO_PG_DIALECT);

  @Override
  public String getSourceType() {
    return SourceConstants.MYSQL_SOURCE_TYPE;
  }

  @Override
  public boolean matchesSourceConfig(SourceConfig sourceConfig) {
    return sourceConfig.getMysqlSourceConfig() != null;
  }

  @Override
  public Map<String, Pair<String, String>> getSortOrder(Dialect dialect) {
    if (dialect == Dialect.POSTGRESQL) {
      return MYSQL_SORT_ORDER_PG_DIALECT;
    }
    return MYSQL_SORT_ORDER;
  }

  @Override
  public ChangeEventContext createChangeEventContext(
      JsonNode changeEvent, Ddl ddl, Ddl shadowTableDdl, String shadowTablePrefix)
      throws ChangeEventConvertorException, InvalidChangeEventException, DroppedTableException {
    return new MySqlChangeEventContext(changeEvent, ddl, shadowTableDdl, shadowTablePrefix);
  }

  @Override
  public ChangeEventSequence createChangeEventSequenceFromChangeEventContext(
      ChangeEventContext changeEventContext)
      throws ChangeEventConvertorException, InvalidChangeEventException {
    return MySqlChangeEventSequence.createFromChangeEvent(changeEventContext);
  }

  @Override
  public ChangeEventSequence createChangeEventSequenceFromShadowTable(
      TransactionContext transactionContext,
      ChangeEventContext changeEventContext,
      Ddl shadowDdl,
      boolean useSqlStatements)
      throws ChangeEventSequenceCreationException, InvalidChangeEventException {
    return MySqlChangeEventSequence.createFromShadowTable(
        transactionContext, changeEventContext, shadowDdl, useSqlStatements);
  }
}
