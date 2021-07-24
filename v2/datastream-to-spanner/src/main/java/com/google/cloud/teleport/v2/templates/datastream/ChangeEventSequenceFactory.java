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

import com.google.cloud.spanner.TransactionContext;
import org.json.JSONObject;

/**
 * Factory classes for ChangeEventSequence classes which provides methods for 1) creating
 * ChangeEventSequence objects for the current ChangeEvent. 2) creating ChangeEventSequence objects
 * for an earlier ChangeEvent by reading from shadow table.
 */
public class ChangeEventSequenceFactory {

  private ChangeEventSequenceFactory() {}

  private static String getSourceType(JSONObject changeEvent) throws InvalidChangeEventException {
    try {
      return changeEvent.getString(DatastreamConstants.EVENT_SOURCE_TYPE_KEY);
    } catch (Exception e) {
      throw new InvalidChangeEventException(e);
    }
  }

  /*
   * Creates a ChangeEventSequence from the current ChangeEvent from ChangeEventContext.
   */
  public static ChangeEventSequence createChangeEventSequenceFromChangeEventContext(
      ChangeEventContext changeEventContext)
      throws ChangeEventConvertorException, InvalidChangeEventException {

    JSONObject changeEvent = changeEventContext.getChangeEvent();
    String sourceType = getSourceType(changeEvent);

    // Create ChangeEventSequence from change event JSON.
    if (DatastreamConstants.MYSQL_SOURCE_TYPE.equals(sourceType)) {
      return MySqlChangeEventSequence.createFromChangeEvent(changeEvent);
    } else if (DatastreamConstants.ORACLE_SOURCE_TYPE.equals(sourceType)) {
      return OracleChangeEventSequence.createFromChangeEvent(changeEvent);
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

    JSONObject changeEvent = changeEventContext.getChangeEvent();
    String sourceType = getSourceType(changeEvent);

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
    }
    throw new InvalidChangeEventException("Unsupported source database: " + sourceType);
  }
}
