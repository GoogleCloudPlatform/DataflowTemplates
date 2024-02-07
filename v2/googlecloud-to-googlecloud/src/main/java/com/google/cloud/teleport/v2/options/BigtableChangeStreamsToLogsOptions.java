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
package com.google.cloud.teleport.v2.options;

// import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions.ReadChangeStreamOptions;
// import
// com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.BigtableSchemaFormat;
// import com.google.cloud.teleport.v2.utils.WriteToGCSUtility.FileFormat;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

// import org.apache.beam.sdk.options.Default;

public interface BigtableChangeStreamsToLogsOptions
    extends DataflowPipelineOptions, ReadChangeStreamOptions {

  // @TemplateParameter.Duration(
  // order = 2,
  // optional = true,
  // description = "Window duration",
  // helpText =
  // "The window duration/size in which data will be written to Cloud Storage. Allowed formats are:
  // Ns (for "
  // + "seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h).",
  // example = "1h")
  //     @Default.String("1h")
  //     String getWindowDuration();
  //
  // void setWindowDuration(String windowDuration);
  //

  // void setUseBase64Values(Boolean useBase64Value);
}
