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
package com.google.cloud.teleport.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

/** Provides options that are supported by all templates. */
public interface CommonTemplateOptions extends PipelineOptions {
  // "Required" annotation is added as a workaround for BEAM-7983.
  @Validation.Required
  @Description(
      "Comma separated algorithms to disable. If this value is set to \"none\" then"
          + " jdk.tls.disabledAlgorithms is set to \"\". Use with care, as the algorithms"
          + " disabled by default are known to have either vulnerabilities or performance issues."
          + " for example: SSLv3, RC4.")
  ValueProvider<String> getDisabledAlgorithms();

  void setDisabledAlgorithms(ValueProvider<String> disabledAlgorithms);

  // "Required" annotation is added as a workaround for BEAM-7983.
  @Validation.Required
  @Description(
      "Comma separated files to stage in the workers. The files can be Cloud storage paths or"
          + " Secret manager secrets. For example:"
          + " gs://your-bucket/file1.txt,projects/project-id/secrets/secret-id/versions/version-id")
  ValueProvider<String> getExtraFilesToStage();

  void setExtraFilesToStage(ValueProvider<String> extraFilesToStage);
}
