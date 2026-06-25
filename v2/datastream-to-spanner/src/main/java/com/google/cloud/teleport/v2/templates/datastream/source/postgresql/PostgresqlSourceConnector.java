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
package com.google.cloud.teleport.v2.templates.datastream.source.postgresql;

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
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.templates.datastream.source.ISourceConnector;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

/** PostgreSQL implementation of {@link ISourceConnector} connector. */
public class PostgresqlSourceConnector implements ISourceConnector {

  @Override
  public String getSourceType() {
    return DatastreamConstants.POSTGRES_SOURCE_TYPE;
  }

  @Override
  public boolean matches(SourceConfig sourceConfig) {
    return sourceConfig.getPostgresqlSourceConfig() != null;
  }

  @Override
  public Map<String, Pair<String, String>> getSortOrder(Dialect dialect) {
    if (dialect == Dialect.POSTGRESQL) {
      return DatastreamConstants.POSTGRES_SORT_ORDER_PG_DIALECT;
    }
    return DatastreamConstants.POSTGRES_SORT_ORDER;
  }

  @Override
  public ChangeEventContext createChangeEventContext(
      JsonNode changeEvent, Ddl ddl, Ddl shadowTableDdl, String shadowTablePrefix)
      throws ChangeEventConvertorException, InvalidChangeEventException, DroppedTableException {
    return new PostgresChangeEventContext(changeEvent, ddl, shadowTableDdl, shadowTablePrefix);
  }

  @Override
  public ChangeEventSequence createChangeEventSequenceFromChangeEventContext(
      ChangeEventContext changeEventContext)
      throws ChangeEventConvertorException, InvalidChangeEventException {
    return PostgresChangeEventSequence.createFromChangeEvent(changeEventContext);
  }

  @Override
  public ChangeEventSequence createChangeEventSequenceFromShadowTable(
      TransactionContext transactionContext,
      ChangeEventContext changeEventContext,
      Ddl shadowDdl,
      boolean useSqlStatements)
      throws ChangeEventSequenceCreationException, InvalidChangeEventException {
    return PostgresChangeEventSequence.createFromShadowTable(
        transactionContext, changeEventContext, shadowDdl, useSqlStatements);
  }
}
