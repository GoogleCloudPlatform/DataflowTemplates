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
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventTypeConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventConvertor;

/**
 * SqlServer implementation of ChangeEventContext that provides implementation of the
 * generateShadowTableMutation method.
 */
public class SqlServerChangeEventContext extends ChangeEventContext {

  public SqlServerChangeEventContext(
      JsonNode changeEvent, Ddl ddl, Ddl shadowTableDdl, String shadowTablePrefix)
      throws ChangeEventConvertorException, InvalidChangeEventException, DroppedTableException {
    super(
        changeEvent,
        ddl,
        ddl.dialect() == Dialect.POSTGRESQL
            ? SqlServerDsToSpSourceConnector.SQLSERVER_SORT_ORDER_PG_DIALECT
            : SqlServerDsToSpSourceConnector.SQLSERVER_SORT_ORDER);
    this.shadowTablePrefix = shadowTablePrefix;
    this.shadowTable = shadowTablePrefix + this.dataTable;

    convertChangeEventToMutation(ddl, shadowTableDdl);
  }

  /*
   * Creates shadow table mutation for SqlServer.
   */
  @Override
  protected Mutation generateShadowTableMutation(Ddl ddl, Ddl shadowDdl)
      throws ChangeEventConvertorException {
    // Get shadow information from change event mutation context
    Mutation.WriteBuilder builder =
        ChangeEventConvertor.changeEventToShadowTableMutationBuilder(
            shadowDdl, changeEvent, shadowTablePrefix);

    // Add timestamp information to shadow table mutation
    Long changeEventTimestamp =
        ChangeEventTypeConvertor.toLong(
            changeEvent,
            SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY,
            /* requiredField= */ true);
    builder
        .set(getSafeShadowColumn(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY))
        .to(Value.int64(changeEventTimestamp));

    /* SqlServer backfill events "can" have LSN value as null.
     * Set the value to a value smaller than any real value.
     */
    String changeEventLSN =
        ChangeEventTypeConvertor.toString(
            changeEvent,
            SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY,
            /* requiredField= */ false);
    if (changeEventLSN == null) {
      changeEventLSN = "";
    }
    // Add lsn information to shadow table mutation
    builder
        .set(getSafeShadowColumn(SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY))
        .to(Value.string(changeEventLSN));

    return builder.build();
  }
}
