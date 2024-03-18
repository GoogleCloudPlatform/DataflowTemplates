/*
 * Copyright (C) 2018 Google LLC
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
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventTypeConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventTypeConvertorException;

/**
 * MySql implementation of ChangeEventContext that provides implementation of the
 * generateShadowTableMutation method.
 */
class MySqlChangeEventContext extends ChangeEventContext {

    public MySqlChangeEventContext(JsonNode changeEvent, Ddl ddl, String shadowTablePrefix)
            throws ChangeEventConvertorException, ChangeEventTypeConvertorException, InvalidChangeEventException {
        this.changeEvent = changeEvent;
        this.shadowTablePrefix = shadowTablePrefix;
        this.dataTable = changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText();
        this.shadowTable = shadowTablePrefix + this.dataTable;
        convertChangeEventToMutation(ddl);
    }

    /*
     * Creates shadow table mutation for MySql.
     */
    @Override
    Mutation generateShadowTableMutation(Ddl ddl)
            throws ChangeEventConvertorException, ChangeEventTypeConvertorException {
        // Get shadow information from change event mutation context
        Mutation.WriteBuilder builder =
                ChangeEventConvertor.changeEventToShadowTableMutationBuilder(
                        ddl, changeEvent, shadowTablePrefix);

        // Add timestamp information to shadow table mutation
        Long changeEventTimestamp =
                ChangeEventTypeConvertor.toLong(
                        changeEvent, DatastreamConstants.MYSQL_TIMESTAMP_KEY, /* requiredField= */ true);
        builder
                .set(DatastreamConstants.MYSQL_TIMESTAMP_SHADOW_INFO.getLeft())
                .to(Value.int64(changeEventTimestamp));

        /* MySql backfill events "can" have log file and log file position as null.
         * Set their value to a value (lexicographically) smaller than any real value.
         */
        String logFile =
                ChangeEventTypeConvertor.toString(
                        changeEvent, DatastreamConstants.MYSQL_LOGFILE_KEY, /* requiredField= */ false);
        if (logFile == null) {
            logFile = "";
        }
        // Add log file information to shadow table mutation
        builder.set(DatastreamConstants.MYSQL_LOGFILE_SHADOW_INFO.getLeft()).to(logFile);

        Long logPosition =
                ChangeEventTypeConvertor.toLong(
                        changeEvent, DatastreamConstants.MYSQL_LOGPOSITION_KEY, /* requiredField= */ false);
        if (logPosition == null) {
            logPosition = new Long(-1);
        }
        // Add logfile position information to shadow table mutation
        builder
                .set(DatastreamConstants.MYSQL_LOGPOSITION_SHADOW_INFO.getLeft())
                .to(Value.int64(logPosition));

        return builder.build();
    }
}
