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
package com.google.cloud.teleport.plugin.terraform;

import com.google.auto.value.AutoValue;
import java.util.List;

@AutoValue
public abstract class TerraformModule {

  static Builder builder() {
    return new AutoValue_TerraformModule.Builder();
  }

  public abstract List<TerraformVariable> getParameters();

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setParameters(List<TerraformVariable> value);

    abstract TerraformModule build();
  }
}
