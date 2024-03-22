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
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventTypeConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of ChangeEventSequence for Oracle database which stores change event sequence
 * information and implements the Comparator method.
 */
class OracleChangeEventSequence extends ChangeEventSequence {

  // Timestamp for change event
  private final Long timestamp;

  // Oracle SCN for change event
  private final Long scn;

  OracleChangeEventSequence(Long timestamp, Long scn) {
    super(DatastreamConstants.ORACLE_SOURCE_TYPE);
    this.timestamp = timestamp;
    this.scn = scn;
  }

  /*
   * Creates OracleChangeEventSequence from change event
   */
  public static OracleChangeEventSequence createFromChangeEvent(ChangeEventContext ctx)
      throws ChangeEventConvertorException, InvalidChangeEventException {

    /* Backfill events from Oracle "can" have only timestamp metadata filled in.
     * Set SCN to a smaller value than any real value
     */
    Long scn;

    scn =
        ChangeEventTypeConvertor.toLong(
            ctx.getChangeEvent(), DatastreamConstants.ORACLE_SCN_KEY, /* requiredField= */ false);
    if (scn == null) {
      scn = new Long(-1);
    }

    // Change events from Oracle have timestamp and SCN filled in always.
    return new OracleChangeEventSequence(
        ChangeEventTypeConvertor.toLong(
            ctx.getChangeEvent(),
            DatastreamConstants.ORACLE_TIMESTAMP_KEY,
            /* requiredField= */ true),
        scn);
  }

  /*
   * Creates a OracleChangeEventSequence by reading from a shadow table.
   */
  public static OracleChangeEventSequence createFromShadowTable(
      final TransactionContext transactionContext, String shadowTable, Key primaryKey)
      throws ChangeEventSequenceCreationException {

    try {
      // Read columns from shadow table
      List<String> readColumnList =
          DatastreamConstants.ORACLE_SORT_ORDER.values().stream()
              .map(p -> p.getLeft())
              .collect(Collectors.toList());
      Struct row = transactionContext.readRow(shadowTable, primaryKey, readColumnList);

      // This is the first event for the primary key and hence the latest event.
      if (row == null) {
        return null;
      }

      return new OracleChangeEventSequence(
          row.getLong(readColumnList.get(0)), row.getLong(readColumnList.get(1)));
    } catch (Exception e) {
      throw new ChangeEventSequenceCreationException(e);
    }
  }

  Long getTimestamp() {
    return timestamp;
  }

  Long getSCN() {
    return scn;
  }

  @Override
  public int compareTo(ChangeEventSequence o) {
    if (!(o instanceof OracleChangeEventSequence)) {
      throw new ChangeEventSequenceComparisonException(
          "Expected: OracleChangeEventSequence; Received: " + o.getClass().getSimpleName());
    }
    OracleChangeEventSequence other = (OracleChangeEventSequence) o;

    int timestampComparisonResult = this.timestamp.compareTo(other.getTimestamp());

    return (timestampComparisonResult != 0)
        ? timestampComparisonResult
        : (scn.compareTo(other.getSCN()));
  }
}
