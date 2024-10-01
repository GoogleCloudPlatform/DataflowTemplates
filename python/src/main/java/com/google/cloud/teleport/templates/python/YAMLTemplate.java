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
package com.google.cloud.teleport.templates.python;

import com.google.cloud.teleport.metadata.HelperImage;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;

/** Placeholder template class for YAMLTemplate in Python. */
@Template(
    name = "Yaml_Template",
    category = TemplateCategory.GET_STARTED,
    type = Template.TemplateType.YAML,
    displayName = "YAML",
    description =
        "The YAML Template is used to run Dataflow pipelines written in Beam YAML. "
            + "The YAML pipeline can be passed to the template directly as a raw string or the location "
            + "of a Beam YAML pipeline file stored in Google Cloud Storage can optionally be passed."
            + "\n\n"
            + "For launching a Beam YAML pipeline directly from the gcloud command line, see "
            + "https://cloud.google.com/sdk/gcloud/reference/dataflow/yaml"
            + "\n\n"
            + "For more information on Beam YAML, see https://beam.apache.org/documentation/sdks/yaml/",
    flexContainerName = "yaml-template",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "A valid Beam YAML pipeline.",
    })
@HelperImage(
    containerName = "job-builder-server",
    filesToCopy = "server.py",
    entryPoint = {"python", "server.py"})
public interface YAMLTemplate {
  @TemplateParameter.Text(
      order = 1,
      name = "yaml_pipeline",
      optional = true,
      description = "Input YAML pipeline spec.",
      hiddenUi = true,
      helpText = "A yaml description of the pipeline to run.")
  String getYamlPipeline();

  @TemplateParameter.GcsReadFile(
      order = 2,
      name = "yaml_pipeline_file",
      optional = true,
      description = "Input YAML pipeline spec file in Cloud Storage.",
      helpText = "A file in Cloud Storage containing a yaml description of the pipeline to run.")
  String getYamlPipelineFile();

  @TemplateParameter.Text(
      order = 3,
      name = "jinja_variables",
      optional = true,
      description = "Input jinja preprocessing variables.",
      helpText =
          "A json dict of variables used when invoking the jinja preprocessor on the provided yaml pipeline.")
  String getJinjaVariables();
}
