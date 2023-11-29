/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.auto.blocks;

import com.google.cloud.teleport.metadata.auto.Consumes;
import com.google.cloud.teleport.metadata.auto.Outputs;
import com.google.cloud.teleport.metadata.auto.TemplateTransform;
import com.google.cloud.teleport.v2.auto.blocks.RowToJSONString.RowToJSONStringOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class RowToJSONString implements TemplateTransform<RowToJSONStringOptions> {
  public interface RowToJSONStringOptions extends PipelineOptions {}

  @Consumes(Row.class)
  @Outputs(String.class)
  public PCollection<String> transform(PCollection<Row> input, RowToJSONStringOptions options) {
    return input.apply("Convert Row to String", ToJson.of());
  }

  @Override
  public Class<RowToJSONStringOptions> getOptionsClass() {
    return RowToJSONStringOptions.class;
  }
}
