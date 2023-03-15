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
import com.google.cloud.teleport.plugin.sample.AtoBOk.AtoBOptions;
import org.apache.beam.sdk.options.Default;

/** Sample template used for testing. */
@Template(
    name = "AtoB",
    displayName = "A to B",
    description = "Send A to B",
    category = TemplateCategory.STREAMING,
    optionsClass = AtoBOptions.class)
public class AtoBOk {

  public interface AtoBOptions {
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

    @TemplateParameter.Boolean(
        order = 3,
        name = "logical",
        description = "Logical",
        helpText = "Define if A goes to B")
    @Default.Boolean(true)
    Boolean logical();

    @TemplateParameter.Boolean(
        order = 4,
        description = "JSON all caps",
        helpText = "Some JSON property.")
    @Default.Boolean(true)
    String getJSON();
  }
}
