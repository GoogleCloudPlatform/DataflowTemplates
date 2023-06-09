/*
 * Copyright (C) 2020 Google LLC
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
package com.google.cloud.teleport.v2.kafka.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * The {@link KafkaCommonOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface KafkaCommonOptions extends PipelineOptions {

  @TemplateParameter.Text(
      order = 1,
      optional = true,
      description = "Secret Store URL",
      helpText = "URL to credentials in Vault")
  String getSecretStoreUrl();

  void setSecretStoreUrl(String secretStoreUrl);

  @TemplateParameter.Text(
      order = 2,
      optional = true,
      description = "Vault token",
      helpText = "Token to use for Vault")
  String getVaultToken();

  void setVaultToken(String vaultToken);
}
