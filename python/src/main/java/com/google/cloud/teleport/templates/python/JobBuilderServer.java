/*
 * Copyright (C) 2024 Google LLC
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

@Template(
    name = "Job_Builder_Server",
    category = TemplateCategory.UTILITIES,
    type = Template.TemplateType.YAML,
    displayName = "YAML",
    description =
        "This Job Builder Server image serves as an HTTP Server for validating pipelines created with Job Builder."
            + "This image is based on the Yaml Template image, but should not be run as a template itself, or released."
            + "as part of the Google-provided Dataflow Templates suite."
            + "For more information on Beam YAML, see https://cloud.google.com/dataflow/docs/guides/job-builder",
    flexContainerName = "job-builder-server",
    entryPoint = {"python", "server.py"},
    filesToCopy = {"server.py", "requirements.txt"},
    stageImageOnly = true)
public interface JobBuilderServer {}
