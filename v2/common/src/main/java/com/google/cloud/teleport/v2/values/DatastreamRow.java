/*
 * Copyright (C) 2021 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.v2.values;

import com.google.api.services.bigquery.model.TableRow;

/**
 * The {@link DatastreamRow} class holds the value of a specific Datastream JSON record.  The
 * data represents 1 CDC event from a given source.  The class is intended to contain all
 * logic used when cleaning or extracting Datastream data details.
 */
// @DefaultCoder(FailsafeElementCoder.class)
public class DatastreamRow {

  private TableRow tableRow;

  private DatastreamRow(TableRow tableRow) {
    this.tableRow = tableRow;
  }

  /**
   * Build a {@code DatastreamRow} for use in pipelines.
   *
   * @param tableRow A TableRow object with Datastream data in it.
   */
  public static DatastreamRow of(TableRow tableRow) {
    return new DatastreamRow(tableRow);
  }

  /* Return the String stream name for the given data. */
  public String getStreamName() {
    return (String) tableRow.get("_metadata_stream");
  }

  /* Return the String source schema name for the given data. */
  public String getSchemaName() {
    return (String) tableRow.get("_metadata_schema");
  }

  /* Return the String source table name for the given data. */
  public String getTableName() {
    return (String) tableRow.get("_metadata_table");
  }

  @Override
  public String toString() {
    return this.tableRow.toString();
  }
}
