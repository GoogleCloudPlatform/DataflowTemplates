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
package com.google.cloud.teleport.v2.neo4j.transforms;

import com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.Target;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Create typed Rows from String rows (from text files). */
public class CastExpandTargetRowFn extends DoFn<Row, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(CastExpandTargetRowFn.class);
  private final Target target;
  private final NodeTarget startNodeTarget;
  private final NodeTarget endNodeTarget;
  private final Schema targetSchema;

  public CastExpandTargetRowFn(
      Target target, NodeTarget startNodeTarget, NodeTarget endNodeTarget, Schema targetSchema) {
    this.target = target;
    this.startNodeTarget = startNodeTarget;
    this.endNodeTarget = endNodeTarget;
    this.targetSchema = targetSchema;
  }

  @ProcessElement
  public void processElement(ProcessContext processContext) {
    Row inputRow = processContext.element();

    List<Object> castVals =
        DataCastingUtils.sourceTextToTargetObjects(
            inputRow, target, startNodeTarget, endNodeTarget);
    if (targetSchema.getFieldCount() != castVals.size()) {
      LOG.error(
          "Unable to parse line.  Expecting {} fields, found {}",
          targetSchema.getFieldCount(),
          castVals.size());
      return;
    }
    Row targetRow = Row.withSchema(targetSchema).attachValues(castVals);
    processContext.output(targetRow);
  }
}
