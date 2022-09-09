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

import com.google.cloud.teleport.v2.neo4j.model.enums.SourceType;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
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

  private final Source source;
  private final Schema schema;
  private final CSVFormat csvFormat;

  public LineToRowFn(Source source, Schema sourceSchema, CSVFormat csvFormat) {
    this.source = source;
    this.schema = sourceSchema;
    this.csvFormat = csvFormat;
  }

  @ProcessElement
  public void processElement(ProcessContext processContext) {

    if (this.source.sourceType == SourceType.text) {

      String line = processContext.element();
      // Note: parser must return objects
      List<Object> strCols = TextParserUtils.parseDelimitedLine(csvFormat, line);
      if (!strCols.isEmpty()) {
        if (this.schema.getFieldCount() != strCols.size()) {
          LOG.error(
              "Unable to parse line.  Expecting {} fields, found {}",
              this.schema.getFieldCount(),
              strCols.size());
        } else {
          Row row = Row.withSchema(this.schema).attachValues(strCols);
          processContext.output(row);
        }
      } else {
        LOG.error("Row was empty!");
      }
    } else {
      LOG.error("Unhandled source type: {}", source.sourceType);
    }
  }
}
