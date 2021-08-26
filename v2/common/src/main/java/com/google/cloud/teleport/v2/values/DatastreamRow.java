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
package com.google.cloud.teleport.v2.values;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Arrays;
import java.util.List;
import org.codehaus.jackson.JsonNode;

/**
 * The {@link DatastreamRow} class holds the value of a specific Datastream JSON record. The data
 * represents 1 CDC event from a given source. The class is intended to contain all logic used when
 * cleaning or extracting Datastream data details.
 */
// @DefaultCoder(FailsafeElementCoder.class)
public class DatastreamRow {

  private TableRow tableRow;
  private JsonNode jsonRow;

  private DatastreamRow(TableRow tableRow, JsonNode jsonRow) {
    this.tableRow = tableRow;
    this.jsonRow = jsonRow;
  }

  /**
   * Build a {@code DatastreamRow} for use in pipelines.
   *
   * @param tableRow A TableRow object with Datastream data in it.
   */
  public static DatastreamRow of(TableRow tableRow) {
    return new DatastreamRow(tableRow, null);
  }

  /**
   * Build a {@code DatastreamRow} for use in pipelines.
   *
   * @param jsonRow A JsonNode object with Datastream data in it.
   */
  public static DatastreamRow of(JsonNode jsonRow) {
    return new DatastreamRow(null, jsonRow);
  }

  /* Return the String stream name for the given data. */
  public String getStreamName() {
    return getStringValue("_metadata_stream");
  }

  /* Return the String source type for the given data (eg. mysql, oracle). */
  public String getSourceType() {
    return getStringValue("_metadata_source_type");
  }

  /* Return the String source schema name for the given data. */
  public String getSchemaName() {
    return getStringValue("_metadata_schema");
  }

  /* Return the String source table name for the given data. */
  public String getTableName() {
    return getStringValue("_metadata_table");
  }

  public String getStringValue(String field) {
    if (this.jsonRow != null) {
      return (String) jsonRow.get(field).getTextValue();
    } else {
      return (String) tableRow.get(field);
    }
  }

  public List<String> getSortFields() {
    if (this.getSourceType() == "mysql") {
      return Arrays.asList("_metadata_timestamp", "_metadata_log_file", "_metadata_log_position");
    } else {
      // Current default is oracle.
      return Arrays.asList("_metadata_timestamp", "_metadata_scn");
    }
  }

  @Override
  public String toString() {
    if (this.jsonRow != null) {
      return this.jsonRow.toString();
    } else {
      return this.tableRow.toString();
    }
  }
}
