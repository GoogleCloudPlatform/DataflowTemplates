/*
 * Copyright (C) 2022 Google LLC
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
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventTypeConvertorException;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of ChangeEventSequence for Postgres database which stores change event sequence
 * information and implements the Comparator method.
 */
class PostgresChangeEventSequence extends ChangeEventSequence {

    // Timestamp for change event
    private final Long timestamp;

    // Postgres LSN for change event
    private final String lsn;

    PostgresChangeEventSequence(Long timestamp, String lsn) {
        super(DatastreamConstants.POSTGRES_SOURCE_TYPE);
        this.timestamp = timestamp;
        this.lsn = lsn;
    }

    /*
     * Creates PostgresChangeEventSequence from change event
     */
    public static PostgresChangeEventSequence createFromChangeEvent(ChangeEventContext ctx)
            throws ChangeEventTypeConvertorException, InvalidChangeEventException {

        /* Backfill events from Postgres "can" have only timestamp metadata filled in.
         * Set LSN to a smaller value than any real value
         */
        String lsn;

        lsn =
                ChangeEventTypeConvertor.toString(
                        ctx.getChangeEvent(), DatastreamConstants.POSTGRES_LSN_KEY, /* requiredField= */ false);
        if (lsn == null) {
            lsn = "";
        }

        // Change events from Postgres have timestamp and lsn filled in always.
        return new PostgresChangeEventSequence(
                ChangeEventTypeConvertor.toLong(
                        ctx.getChangeEvent(),
                        DatastreamConstants.POSTGRES_TIMESTAMP_KEY,
                        /* requiredField= */ true),
                lsn);
    }

    /*
     * Creates a PostgresChangeEventSequence by reading from a shadow table.
     */
    public static PostgresChangeEventSequence createFromShadowTable(
            final TransactionContext transactionContext, String shadowTable, Key primaryKey)
            throws ChangeEventSequenceCreationException {

        try {
            // Read columns from shadow table
            List<String> readColumnList =
                    DatastreamConstants.POSTGRES_SORT_ORDER.values().stream()
                            .map(p -> p.getLeft())
                            .collect(Collectors.toList());
            Struct row = transactionContext.readRow(shadowTable, primaryKey, readColumnList);

            // This is the first event for the primary key and hence the latest event.
            if (row == null) {
                return null;
            }

            return new PostgresChangeEventSequence(
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
     * Postgres LSN values are of the form 16/2A50F3.
     */
    Long getParsedLSN(int index) {
        if (lsn == "") {
            return 0L;
        }
        String[] parts = lsn.split("/");
        if (parts.length <= index) {
            return 0L;
        }
        return Long.parseLong(parts[index], 16);
    }

    @Override
    public int compareTo(ChangeEventSequence o) {
        if (!(o instanceof PostgresChangeEventSequence)) {
            throw new ChangeEventSequenceComparisonException(
                    "Expected: PostgresChangeEventSequence; Received: " + o.getClass().getSimpleName());
        }
        PostgresChangeEventSequence other = (PostgresChangeEventSequence) o;

        int timestampComparisonResult = this.timestamp.compareTo(other.getTimestamp());

        if (timestampComparisonResult != 0) {
            return timestampComparisonResult;
        } else {
            int parsedLeftLSNComparisonResult = this.getParsedLSN(0).compareTo(other.getParsedLSN(0));
            return parsedLeftLSNComparisonResult != 0
                    ? parsedLeftLSNComparisonResult
                    : this.getParsedLSN(1).compareTo(other.getParsedLSN(1));
        }
    }
}
