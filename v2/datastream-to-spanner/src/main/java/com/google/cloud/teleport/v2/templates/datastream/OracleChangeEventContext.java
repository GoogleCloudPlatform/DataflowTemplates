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
import com.google.cloud.teleport.v2.templates.spanner.ddl.Ddl;

/**
 * Oracle implementation of ChangeEventContext that provides implementation of the
 * generateShadowTableMutation method.
 */
class OracleChangeEventContext extends ChangeEventContext {

  public OracleChangeEventContext(JsonNode changeEvent, Ddl ddl, String shadowTablePrefix)
      throws ChangeEventConvertorException, InvalidChangeEventException {
    this.changeEvent = changeEvent;
    this.shadowTablePrefix = shadowTablePrefix;
    this.dataTable = changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText();
    this.shadowTable = shadowTablePrefix + this.dataTable;
    convertChangeEventToMutation(ddl);
  }

  /*
   * Creates shadow table mutation for Oracle.
   */
  @Override
  Mutation generateShadowTableMutation(Ddl ddl)
      throws ChangeEventConvertorException, InvalidChangeEventException {
    // Get shadow information from change event mutation context
    Mutation.WriteBuilder builder =
        ChangeEventConvertor.changeEventToShadowTableMutationBuilder(
            ddl, changeEvent, shadowTablePrefix);

    // Add timestamp information to shadow table mutation
    Long changeEventTimestamp =
        ChangeEventTypeConvertor.toLong(
            changeEvent, DatastreamConstants.ORACLE_TIMESTAMP_KEY, /* requiredField= */ true);
    builder
        .set(DatastreamConstants.ORACLE_TIMESTAMP_SHADOW_INFO.getLeft())
        .to(Value.int64(changeEventTimestamp));

    /* Oracle backfill events "can" have SCN value as null.
     * Set the value to a value smaller than any real value.
     */
    Long changeEventSCN =
        ChangeEventTypeConvertor.toLong(
            changeEvent, DatastreamConstants.ORACLE_SCN_KEY, /* requiredField= */ false);
    if (changeEventSCN == null) {
      changeEventSCN = new Long(-1);
    }
    // Add scn information to shadow table mutation
    builder
        .set(DatastreamConstants.ORACLE_SCN_SHADOW_INFO.getLeft())
        .to(Value.int64(changeEventSCN));

    return builder.build();
  }
}
