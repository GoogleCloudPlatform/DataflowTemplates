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
package com.google.cloud.teleport.plugin.sample;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;

@Template(
    name = "AtoBNestedFlex",
    displayName = "A to B nested flex",
    description = {"Streaming Template that sends A to B.", "But it can also send B to C."},
    category = TemplateCategory.STREAMING,
    optionsClass = AtoBNestedFlex.AtoBNestedFlexOptions.class,
    preview = true,
    requirements = "Requires the customer to use Dataflow",
    flexContainerName = "parent/AtoBNestedFlex")
public class AtoBNestedFlex {
  public interface AtoBNestedFlexOptions {}
}
