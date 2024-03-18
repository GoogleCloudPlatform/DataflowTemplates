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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventTypeConvertorException;

/**
 * Factory classes for ChangeEventSequence classes which provides methods for 1) creating
 * ChangeEventSequence objects for the current ChangeEvent. 2) creating ChangeEventSequence objects
 * for an earlier ChangeEvent by reading from shadow table.
 */
public class ChangeEventSequenceFactory {

    private ChangeEventSequenceFactory() {
    }

    private static String getSourceType(JsonNode changeEvent) throws InvalidChangeEventException {
        try {
            return changeEvent.get(DatastreamConstants.EVENT_SOURCE_TYPE_KEY).asText();
        } catch (Exception e) {
            throw new InvalidChangeEventException(e);
        }
    }

    /*
     * Creates a ChangeEventSequence from the current ChangeEvent from ChangeEventContext.
     */
    public static ChangeEventSequence createChangeEventSequenceFromChangeEventContext(
            ChangeEventContext changeEventContext)
            throws ChangeEventTypeConvertorException, InvalidChangeEventException {

        String sourceType = getSourceType(changeEventContext.getChangeEvent());

        // Create ChangeEventSequence from change event JSON.
        if (DatastreamConstants.MYSQL_SOURCE_TYPE.equals(sourceType)) {
            return MySqlChangeEventSequence.createFromChangeEvent(changeEventContext);
        } else if (DatastreamConstants.ORACLE_SOURCE_TYPE.equals(sourceType)) {
            return OracleChangeEventSequence.createFromChangeEvent(changeEventContext);
        } else if (DatastreamConstants.POSTGRES_SOURCE_TYPE.equals(sourceType)) {
            return PostgresChangeEventSequence.createFromChangeEvent(changeEventContext);
        }
        throw new InvalidChangeEventException("Unsupported source database: " + sourceType);
    }

    /*
     * Create a ChangeEventSequence object for an earlier event by reading
     * from shadow tables.
     */
    public static ChangeEventSequence createChangeEventSequenceFromShadowTable(
            final TransactionContext transactionContext, final ChangeEventContext changeEventContext)
            throws ChangeEventSequenceCreationException, InvalidChangeEventException {

        String sourceType = getSourceType(changeEventContext.getChangeEvent());

        if (DatastreamConstants.MYSQL_SOURCE_TYPE.equals(sourceType)) {
            return MySqlChangeEventSequence.createFromShadowTable(
                    transactionContext,
                    changeEventContext.getShadowTable(),
                    changeEventContext.getPrimaryKey());
        } else if (DatastreamConstants.ORACLE_SOURCE_TYPE.equals(sourceType)) {
            return OracleChangeEventSequence.createFromShadowTable(
                    transactionContext,
                    changeEventContext.getShadowTable(),
                    changeEventContext.getPrimaryKey());
        } else if (DatastreamConstants.POSTGRES_SOURCE_TYPE.equals(sourceType)) {
            return PostgresChangeEventSequence.createFromShadowTable(
                    transactionContext,
                    changeEventContext.getShadowTable(),
                    changeEventContext.getPrimaryKey());
        }
        throw new InvalidChangeEventException("Unsupported source database: " + sourceType);
    }
}
