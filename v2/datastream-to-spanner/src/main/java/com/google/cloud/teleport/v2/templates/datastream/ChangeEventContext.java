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
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.templates.spanner.ddl.Ddl;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ChangeEventContext class converts change events to Cloud Spanner mutations and stores all
 * intermediatory objects prior to applying them to Cloud Spanner.
 */
public abstract class ChangeEventContext {

  private static final Logger LOG = LoggerFactory.getLogger(ChangeEventContext.class);

  // The JsonNode representation of the change event.
  protected JsonNode changeEvent;

  // Cloud Spanner mutation for the change event.
  protected Mutation dataMutation;

  // Cloud Spanner primary key for the change event.
  protected Key primaryKey;

  // Shadow table for the change event.
  protected String shadowTable;

  // Cloud Spanner mutation for the shadow table corresponding to this change event.
  protected Mutation shadowTableMutation;

  // The prefix to be applied for shadow table.
  protected String shadowTablePrefix;

  // Data table for the change event.
  protected String dataTable;

  // Abstract method to generate shadow table mutation.
  abstract Mutation generateShadowTableMutation(Ddl ddl)
      throws ChangeEventConvertorException, InvalidChangeEventException;

  // Helper method to convert change event to mutation.
  protected void convertChangeEventToMutation(Ddl ddl)
      throws ChangeEventConvertorException, InvalidChangeEventException {
    ChangeEventConvertor.convertChangeEventColumnKeysToLowerCase(changeEvent);
    ChangeEventConvertor.verifySpannerSchema(ddl, changeEvent);
    this.primaryKey = ChangeEventConvertor.changeEventToPrimaryKey(ddl, changeEvent);
    this.dataMutation = ChangeEventConvertor.changeEventToMutation(ddl, changeEvent);
    this.shadowTableMutation = generateShadowTableMutation(ddl);
  }

  public JsonNode getChangeEvent() {
    return changeEvent;
  }

  // Returns an array of data and shadow table mutations.
  public Iterable<Mutation> getMutations() {
    return Arrays.asList(dataMutation, shadowTableMutation);
  }

  // Getter method for the primary key of the change event.
  public Key getPrimaryKey() {
    return primaryKey;
  }

  // Getter method for shadow mutation (used for tests).
  public Mutation getShadowTableMutation() {
    return shadowTableMutation;
  }

  // Getter method for the shadow table.
  public String getShadowTable() {
    return shadowTable;
  }
}
