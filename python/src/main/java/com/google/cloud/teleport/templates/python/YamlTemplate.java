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

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;

/** Placeholder template class for YamlTemplate in Python. */
@Template(
    name = "Yaml_Template",
    category = TemplateCategory.GET_STARTED,
    type = Template.TemplateType.PYTHON,
    displayName = "Yaml Template (Experimental)",
    description =
        "Yaml pipeline. Reads yaml from Cloud Storage and dynamically expands yaml into "
            + "Beam pipeline graph.",
    flexContainerName = "yaml-template",
    contactInformation = "https://cloud.google.com/support")
public interface YamlTemplate {
  @TemplateParameter.GcsReadFile(
      order = 1,
      name = "yaml",
      description = "Input yaml file in Cloud Storage.",
      helpText = "The input yaml file Dataflow reads from.")
  String getYaml();
}
