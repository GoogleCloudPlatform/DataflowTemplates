/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.templates.datastream;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.constants.DatastreamConstants;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventTypeConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import com.google.cloud.teleport.v2.spanner.migrations.spanner.SpannerReadUtils;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of ChangeEventSequence for MySql database which stores change event sequence
 * information and implements the comparison method.
 */
class MySqlChangeEventSequence extends ChangeEventSequence {

  private static final Logger LOG = LoggerFactory.getLogger(MySqlChangeEventSequence.class);

  // Timestamp for change event
  private final Long timestamp;

  // Log file for change event
  private final String logFile;

  // Log position for change event
  private final Long logPosition;

  MySqlChangeEventSequence(Long timestamp, String logFile, Long logPosition) {
    super(DatastreamConstants.MYSQL_SOURCE_TYPE);
    this.timestamp = timestamp;
    this.logFile = logFile;
    this.logPosition = logPosition;
  }

  /*
   * Creates MySqlChangeEventSequence from change event
   */
  public static MySqlChangeEventSequence createFromChangeEvent(ChangeEventContext ctx)
      throws ChangeEventConvertorException, InvalidChangeEventException {

    /* Dump events from MySql only has timestamp metadata filled in. They don't have
     * logfile and logposition metadata.
     * Set logFile, logPosition smaller than any real value so that Dump events
     * are smaller than change event in the same timestamp.
     */
    String logFile;
    Long logPosition;

    logFile =
        ChangeEventTypeConvertor.toString(
            ctx.getChangeEvent(),
            DatastreamConstants.MYSQL_LOGFILE_KEY,
            /* requiredField= */ false);
    if (logFile == null) {
      logFile = "";
    }

    logPosition =
        ChangeEventTypeConvertor.toLong(
            ctx.getChangeEvent(),
            DatastreamConstants.MYSQL_LOGPOSITION_KEY,
            /* requiredField= */ false);
    if (logPosition == null) {
      logPosition = new Long(-1);
    }

    // Create MySqlChangeEventSequence from JSON keys in change event.
    return new MySqlChangeEventSequence(
        ChangeEventTypeConvertor.toLong(
            ctx.getChangeEvent(),
            DatastreamConstants.MYSQL_TIMESTAMP_KEY,
            /* requiredField= */ true),
        logFile,
        logPosition);
  }

  /*
   * Creates a MySqlChangeEventSequence by reading from a shadow table.
   * @param transactionContext The transaction context to use for reading from the shadow table
   * @param context The change event context with resolved safe shadow column names
   * @param useSqlStatements If true, performs shadow table read using SQL statement with exclusive lock on row
   */
  public static MySqlChangeEventSequence createFromShadowTable(
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
              context.getSafeShadowColumn(DatastreamConstants.MYSQL_TIMESTAMP_KEY),
              context.getSafeShadowColumn(DatastreamConstants.MYSQL_LOGFILE_KEY),
              context.getSafeShadowColumn(DatastreamConstants.MYSQL_LOGPOSITION_KEY));
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
      return new MySqlChangeEventSequence(
          row.getLong(readColumnList.get(0)),
          row.getString(readColumnList.get(1)),
          row.getLong(readColumnList.get(2)));
    } catch (Exception e) {
      throw new ChangeEventSequenceCreationException(e);
    }
  }

  Long getTimestamp() {
    return timestamp;
  }

  String getLogFile() {
    return logFile;
  }

  Long getLogPosition() {
    return logPosition;
  }

  @Override
  public int compareTo(ChangeEventSequence o) {
    if (!(o instanceof MySqlChangeEventSequence)) {
      throw new ChangeEventSequenceComparisonException(
          "Expected: MySqlChangeEventSequence; Received: " + o.getClass().getSimpleName());
    }
    MySqlChangeEventSequence other = (MySqlChangeEventSequence) o;

    // For backfill events logfile will be null/empty.
    // These should always be treated as before the CDC events
    if (this.logFile == null && other.getLogFile() == null) {
      // if two backfill events happen to come, order by time of reading
      return this.timestamp.compareTo(other.getTimestamp());
    }
    if (this.logFile == null) {
      return -1; // current entry backfill - move current before other
    }
    if (other.getLogFile() == null) {
      return 1; // other entry is backfill - move current after other
    }

    int logFileComparisonResult = this.logFile.compareTo(other.getLogFile());
    if (logFileComparisonResult != 0) {
      return logFileComparisonResult;
    }

    int logPositionComparisonResult = this.logPosition.compareTo(other.getLogPosition());
    if (logPositionComparisonResult != 0) {
      return logPositionComparisonResult;
    }

    int timestampComparisonResult = this.timestamp.compareTo(other.getTimestamp());
    if (timestampComparisonResult == 0) {
      LOG.warn(
          "encountered two events with same log file: {} and position: {} and timestamp:{}",
          this.logFile,
          this.logPosition,
          this.timestamp);
    }
    return timestampComparisonResult;
  }

  @Override
  public String toString() {
    return "MySqlChangeEventSequence{"
        + "timestamp="
        + timestamp
        + ", logFile="
        + logFile
        + ", logPosition="
        + logPosition
        + '}';
  }
}
