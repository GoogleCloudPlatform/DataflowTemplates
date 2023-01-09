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
package com.google.cloud.teleport.plugin.sample;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.plugin.sample.AtoBMissingAnnotation.AtoBOptionsMissingAnnotation;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/** Sample template used for testing. */
@Template(
    name = "AtoBMissing",
    displayName = "A to B Missing Annotation",
    description = "Send A to B Missing Annotation",
    category = TemplateCategory.STREAMING,
    optionsClass = AtoBOptionsMissingAnnotation.class)
public class AtoBMissingAnnotation {

  interface AtoBOptionsMissingAnnotation {
    @TemplateParameter.BigQueryTable(
        order = 2,
        name = "to",
        description = "to",
        helpText = "Table to send data to",
        example = "b")
    String to();

    @TemplateParameter.Text(
        order = 1,
        name = "from",
        description = "from",
        helpText = "Define where to get data from",
        example = "a")
    String from();

    @Description("Logical value")
    @Default.Boolean(true)
    Boolean logical();
  }
}
