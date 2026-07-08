/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.dofn;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.utils.SchemaUtils;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DoFn to build the schema DAG and log root tables. */
public class BuildSchemaDagFn extends DoFn<DataGeneratorSchema, DataGeneratorSchema> {
  private static final Logger LOG = LoggerFactory.getLogger(BuildSchemaDagFn.class);

  @ProcessElement
  public void processElement(
      @Element DataGeneratorSchema schema, OutputReceiver<DataGeneratorSchema> receiver) {
    DataGeneratorSchema processedSchema = SchemaUtils.generateSchemaDAG(schema);

    List<String> rootTables =
        processedSchema.tables().values().stream()
            .filter(DataGeneratorTable::isRoot)
            .map(DataGeneratorTable::name)
            .collect(Collectors.toList());
    LOG.info("Root tables in the job: {}", rootTables);

    receiver.output(processedSchema);
  }
}
