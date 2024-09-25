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
package com.google.cloud.teleport.v2.neo4j.providers.text;

import com.google.cloud.teleport.v2.neo4j.utils.TextParserUtils;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Leverages CSV parser to turn delimited lines to PCollection rows. */
public class LineToRowFn extends DoFn<String, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(LineToRowFn.class);

  private final Schema schema;
  private final CSVFormat csvFormat;

  public LineToRowFn(Schema sourceSchema, CSVFormat csvFormat) {
    this.schema = sourceSchema;
    this.csvFormat = csvFormat;
  }

  @ProcessElement
  public void processElement(ProcessContext processContext) {
    String line = processContext.element();
    // Note: parser must return objects
    List<Object> strCols = TextParserUtils.parseDelimitedLine(csvFormat, line);
    if (strCols.isEmpty()) {
      LOG.error("Row was empty!");
      return;
    }
    // If there are more defined fields than fields to map, error (message) out
    if (this.schema.getFieldCount() > strCols.size()) {
      LOG.error(
          "Unable to parse line.  Expecting {} fields, found {}",
          this.schema.getFieldCount(),
          strCols.size());
      return;
    }
    // truncate input column names to the number of defined field names
    // this should in the future be moved into a method that
    // extracts the matching columns from the given header name
    strCols = strCols.subList(0, this.schema.getFieldCount());
    Row row = Row.withSchema(this.schema).attachValues(strCols);
    processContext.output(row);
  }
}
