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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.dofn.ApplyOverridesFn;
import com.google.cloud.teleport.v2.templates.dofn.BuildSchemaDagFn;
import com.google.cloud.teleport.v2.templates.dofn.FetchSchemaFn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.SchemaConfig;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that loads the {@link DataGeneratorSchema} from the sink as a side input.
 */
public class SchemaLoader extends PTransform<PBegin, PCollectionView<DataGeneratorSchema>> {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaLoader.class);
  private final SinkType sinkType;
  private final String sinkOptionsPath;
  private final Integer insertQps;
  private final Integer updateQps;
  private final Integer deleteQps;
  private final SchemaConfig schemaConfig;

  public SchemaLoader(
      SinkType sinkType,
      String sinkOptionsPath,
      Integer insertQps,
      Integer updateQps,
      Integer deleteQps,
      SchemaConfig schemaConfig) {
    this.sinkType = sinkType;
    this.sinkOptionsPath = sinkOptionsPath;
    this.insertQps = insertQps;
    this.updateQps = updateQps;
    this.deleteQps = deleteQps;
    this.schemaConfig = schemaConfig;
  }

  @Override
  public PCollectionView<DataGeneratorSchema> expand(PBegin input) {
    return input
        .apply("CreateSinkType", Create.of(sinkType))
        .apply("FetchSchemaFromDb", ParDo.of(new FetchSchemaFn(sinkType, sinkOptionsPath)))
        .apply(
            "ApplyOverrides",
            ParDo.of(new ApplyOverridesFn(schemaConfig, insertQps, updateQps, deleteQps)))
        .apply("BuildSchemaDAG", ParDo.of(new BuildSchemaDagFn()))
        .apply("ViewAsSingleton", View.asSingleton());
  }
}
