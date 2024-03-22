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
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;

/** Factory classes that provides creation methods for ChangeEventContext. */
public class ChangeEventContextFactory {

  private ChangeEventContextFactory() {}

  private static String getSourceType(JsonNode changeEvent) throws InvalidChangeEventException {
    try {
      return changeEvent.get(DatastreamConstants.EVENT_SOURCE_TYPE_KEY).asText();
    } catch (Exception e) {
      throw new InvalidChangeEventException(e);
    }
  }

  /*
   * Creates ChangeEventContext depending on the change event type.
   */
  public static ChangeEventContext createChangeEventContext(
      JsonNode changeEvent, Ddl ddl, String shadowTablePrefix, String sourceType)
      throws ChangeEventConvertorException, InvalidChangeEventException {

    if (!sourceType.equals(getSourceType(changeEvent))) {
      throw new InvalidChangeEventException(
          "Change event with invalid source. "
              + "Actual("
              + getSourceType(changeEvent)
              + "), Expected("
              + sourceType
              + ")");
    }

    if (DatastreamConstants.MYSQL_SOURCE_TYPE.equals(sourceType)) {
      return new MySqlChangeEventContext(changeEvent, ddl, shadowTablePrefix);
    } else if (DatastreamConstants.ORACLE_SOURCE_TYPE.equals(sourceType)) {
      return new OracleChangeEventContext(changeEvent, ddl, shadowTablePrefix);
    } else if (DatastreamConstants.POSTGRES_SOURCE_TYPE.equals(sourceType)) {
      return new PostgresChangeEventContext(changeEvent, ddl, shadowTablePrefix);
    }

    throw new InvalidChangeEventException("Unsupported source database: " + sourceType);
  }
}
