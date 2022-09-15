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
package com.google.cloud.teleport.v2.neo4j.providers.text;

import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transforms list of object to PCollection<Row>. */
public class ListOfStringToRowFn extends DoFn<List<Object>, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(ListOfStringToRowFn.class);

  private final Schema schema;

  public ListOfStringToRowFn(Schema sourceSchema) {
    this.schema = sourceSchema;
  }

  @ProcessElement
  public void processElement(ProcessContext processContext) {

    List<Object> strCols = processContext.element();
    if (this.schema.getFieldCount() != strCols.size()) {
      LOG.error(
          "Row field count mismatch, expecting: {}, row: {}",
          this.schema.getFieldCount(),
          StringUtils.join(strCols.size(), ","));
    } else {
      Row row = Row.withSchema(this.schema).addValues(strCols).build();
      processContext.output(row);
    }
  }
}
