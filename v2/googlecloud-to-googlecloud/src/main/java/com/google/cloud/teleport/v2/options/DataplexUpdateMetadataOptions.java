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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/** Pipeline options used by Dataplex templates to configure Dataplex metadata updates. */
public interface DataplexUpdateMetadataOptions extends PipelineOptions {

  @TemplateParameter.Boolean(
      order = 1,
      optional = true,
      description = "Update Dataplex metadata.",
      helpText =
          "Whether to update Dataplex metadata for the newly created entities. Only supported for Cloud Storage destination. If enabled, the pipeline will automatically copy the schema from source to the destination Dataplex entities, and the automated Dataplex Discovery won't run for them. Use this flag in cases where you have managed schema at the source.")
  @Default.Boolean(false)
  @Required
  Boolean getUpdateDataplexMetadata();

  void setUpdateDataplexMetadata(Boolean updateDataplexMetadata);
}
