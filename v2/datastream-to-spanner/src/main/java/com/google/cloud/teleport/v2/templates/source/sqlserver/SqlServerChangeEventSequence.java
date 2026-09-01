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

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventTypeConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import com.google.cloud.teleport.v2.spanner.migrations.spanner.SpannerReadUtils;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequence;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequenceComparisonException;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequenceCreationException;
import java.util.List;

/**
 * Implementation of ChangeEventSequence for SqlServer database which stores change event sequence
 * information and implements the Comparator method.
 */
class SqlServerChangeEventSequence extends ChangeEventSequence {

  // Timestamp for change event
  private final Long timestamp;

  // SqlServer LSN for change event
  private final String lsn;

  SqlServerChangeEventSequence(Long timestamp, String lsn) {
    super("sqlserver");
    this.timestamp = timestamp;
    this.lsn = lsn;
  }

  /*
   * Creates SqlServerChangeEventSequence from change event
   */
  public static SqlServerChangeEventSequence createFromChangeEvent(ChangeEventContext ctx)
      throws ChangeEventConvertorException, InvalidChangeEventException {

    /* Backfill events from SqlServer "can" have only timestamp metadata filled in.
     * Set LSN to a smaller value than any real value
     */
    String lsn =
        ChangeEventTypeConvertor.toString(
            ctx.getChangeEvent(),
            SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY,
            /* requiredField= */ false);
    if (lsn == null) {
      lsn = "";
    }

    // Change events from SqlServer have timestamp and lsn filled in always.
    return new SqlServerChangeEventSequence(
        ChangeEventTypeConvertor.toLong(
            ctx.getChangeEvent(),
            SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY,
            /* requiredField= */ true),
        lsn);
  }

  /*
   * Creates a SqlServerChangeEventSequence by reading from a shadow table.
   */
  public static SqlServerChangeEventSequence createFromShadowTable(
      final TransactionContext transactionContext,
      ChangeEventContext context,
      Ddl shadowTableDdl,
      boolean useSqlStatements)
      throws ChangeEventSequenceCreationException {

    try {
      String shadowTable = context.getShadowTable();
      Key primaryKey = context.getPrimaryKey();
      // Read columns from shadow table
      List<String> readColumnList =
          java.util.Arrays.asList(
              context.getSafeShadowColumn(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY),
              context.getSafeShadowColumn(SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY));
      Struct row;
      // TODO: After beam release, use the latest client lib version which supports setting lock
      // hints via the read api. SQL string generation should be removed.
      if (useSqlStatements) {
        Statement sql =
            SpannerReadUtils.generateReadSQLWithExclusiveLock(
                shadowTable, readColumnList, primaryKey, shadowTableDdl);
        ResultSet resultSet = transactionContext.executeQuery(sql);
        if (!resultSet.next()) {
          return null;
        }
        row = resultSet.getCurrentRowAsStruct();
      } else {
        // Use direct row read
        row = transactionContext.readRow(shadowTable, primaryKey, readColumnList);
      }
      // This is the first event for the primary key and hence the latest event.
      if (row == null) {
        return null;
      }

      return new SqlServerChangeEventSequence(
          row.getLong(readColumnList.get(0)), row.getString(readColumnList.get(1)));
    } catch (Exception e) {
      throw new ChangeEventSequenceCreationException(e);
    }
  }

  Long getTimestamp() {
    return timestamp;
  }

  String getLSN() {
    return lsn;
  }

  /*
   * For SqlServer we compare strings for LSN. LSN format is like 00000021:0000010f:0001.
   * String comparison is sufficient for Datastream's string representation.
   */
  @Override
  public int compareTo(ChangeEventSequence o) {
    if (!(o instanceof SqlServerChangeEventSequence)) {
      throw new ChangeEventSequenceComparisonException(
          "Expected: SqlServerChangeEventSequence; Received: " + o.getClass().getSimpleName());
    }
    SqlServerChangeEventSequence other = (SqlServerChangeEventSequence) o;

    int timestampComparisonResult = this.timestamp.compareTo(other.getTimestamp());

    if (timestampComparisonResult != 0) {
      return timestampComparisonResult;
    } else {
      return this.lsn.compareTo(other.getLSN());
    }
  }

  @Override
  public String toString() {
    return "SqlServerChangeEventSequence{" + "timestamp=" + timestamp + ", lsn=" + lsn + '}';
  }
}
