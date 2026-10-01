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
package com.google.cloud.teleport.v2.templates.source.oracle;

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

/** Oracle implementation of {@link IDsToSpSourceConnector} connector. */
public class OracleDsToSpSourceConnector implements IDsToSpSourceConnector {

  /* List of Event keys, Shadow table information related to sort order in Oracle database. */
  public static final String ORACLE_TIMESTAMP_KEY = "_metadata_timestamp";
  public static final Pair<String, String> ORACLE_TIMESTAMP_SHADOW_INFO =
      Pair.of("timestamp", "INT64");
  public static final Pair<String, String> ORACLE_TIMESTAMP_SHADOW_INFO_PG_DIALECT =
      Pair.of("timestamp", "bigint");
  public static final String ORACLE_SCN_KEY = "_metadata_scn";
  public static final Pair<String, String> ORACLE_SCN_SHADOW_INFO = Pair.of("scn", "INT64");
  public static final Pair<String, String> ORACLE_SCN_SHADOW_INFO_PG_DIALECT =
      Pair.of("scn", "bigint");
  /* Mapping from Event keys to shadow table information for Oracle database with gsql dialect*/
  public static final Map<String, Pair<String, String>> ORACLE_SORT_ORDER =
      ImmutableMap.of(
          OracleDsToSpSourceConnector.ORACLE_TIMESTAMP_KEY,
          OracleDsToSpSourceConnector.ORACLE_TIMESTAMP_SHADOW_INFO,
          OracleDsToSpSourceConnector.ORACLE_SCN_KEY,
          OracleDsToSpSourceConnector.ORACLE_SCN_SHADOW_INFO);
  /* Mapping from Event keys to shadow table information for Oracle database with postgres dialect*/
  public static final Map<String, Pair<String, String>> ORACLE_SORT_ORDER_PG_DIALECT =
      ImmutableMap.of(
          OracleDsToSpSourceConnector.ORACLE_TIMESTAMP_KEY,
          OracleDsToSpSourceConnector.ORACLE_TIMESTAMP_SHADOW_INFO_PG_DIALECT,
          OracleDsToSpSourceConnector.ORACLE_SCN_KEY,
          OracleDsToSpSourceConnector.ORACLE_SCN_SHADOW_INFO_PG_DIALECT);

  @Override
  public String getSourceType() {
    return SourceConstants.ORACLE_SOURCE_TYPE;
  }

  @Override
  public boolean matchesSourceConfig(SourceConfig sourceConfig) {
    return sourceConfig.getOracleSourceConfig() != null;
  }

  @Override
  public Map<String, Pair<String, String>> getSortOrder(Dialect dialect) {
    if (dialect == Dialect.POSTGRESQL) {
      return ORACLE_SORT_ORDER_PG_DIALECT;
    }
    return ORACLE_SORT_ORDER;
  }

  @Override
  public ChangeEventContext createChangeEventContext(
      JsonNode changeEvent, Ddl ddl, Ddl shadowTableDdl, String shadowTablePrefix)
      throws ChangeEventConvertorException, InvalidChangeEventException, DroppedTableException {
    return new OracleChangeEventContext(changeEvent, ddl, shadowTableDdl, shadowTablePrefix);
  }

  @Override
  public ChangeEventSequence createChangeEventSequenceFromChangeEventContext(
      ChangeEventContext changeEventContext)
      throws ChangeEventConvertorException, InvalidChangeEventException {
    return OracleChangeEventSequence.createFromChangeEvent(changeEventContext);
  }

  @Override
  public ChangeEventSequence createChangeEventSequenceFromShadowTable(
      TransactionContext transactionContext,
      ChangeEventContext changeEventContext,
      Ddl shadowDdl,
      boolean useSqlStatements)
      throws ChangeEventSequenceCreationException, InvalidChangeEventException {
    return OracleChangeEventSequence.createFromShadowTable(
        transactionContext, changeEventContext, shadowDdl, useSqlStatements);
  }
}
