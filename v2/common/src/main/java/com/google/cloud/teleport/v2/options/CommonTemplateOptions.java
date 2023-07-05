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
import org.apache.beam.sdk.options.PipelineOptions;

/** Provides options that are supported by all templates. */
public interface CommonTemplateOptions extends PipelineOptions {

  @TemplateParameter.Text(
      order = 31,
      optional = true,
      description = "Disabled algorithms to override jdk.tls.disabledAlgorithms",
      helpText =
          "Comma separated algorithms to disable. If this value is set to \"none\" then jdk.tls.disabledAlgorithms is set to \"\". Use with care, as the algorithms disabled by default are known to have either vulnerabilities or performance issues. For example: SSLv3, RC4.")
  String getDisabledAlgorithms();

  void setDisabledAlgorithms(String disabledAlgorithms);

  @TemplateParameter.Text(
      order = 32,
      optional = true,
      regexes = {
        "^((gs:\\/\\/[^\\n\\r,]+|projects\\/[^\\n\\r\\/]+\\/secrets\\/[^\\n\\r\\/]+\\/versions\\/[^\\n\\r\\/]+),)*(gs:\\/\\/[^\\n\\r,]+|projects\\/[^\\n\\r\\/]+\\/secrets\\/[^\\n\\r\\/]+\\/versions\\/[^\\n\\r\\/]+)$"
      },
      description = "Extra files to stage in the workers",
      helpText =
          "Comma separated Cloud Storage paths or Secret Manager secrets for files to stage "
              + "in the worker. These files will be saved under the `/extra_files` directory in each "
              + "worker.",
      example =
          "gs://your-bucket/file.txt,projects/project-id/secrets/secret-id/versions/version-id")
  String getExtraFilesToStage();

  void setExtraFilesToStage(String extraFilesToStage);
}
