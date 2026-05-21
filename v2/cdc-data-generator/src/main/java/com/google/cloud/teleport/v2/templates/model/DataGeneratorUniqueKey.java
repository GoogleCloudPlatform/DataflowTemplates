/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.model;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;

/** Represents a unique key/index in the data generator schema. */
@AutoValue
public abstract class DataGeneratorUniqueKey implements Serializable {

  /** The name of the unique key/index. */
  public abstract String name();

  /** The columns that make up the unique key. */
  public abstract ImmutableList<String> columns();

  public static Builder builder() {
    return new AutoValue_DataGeneratorUniqueKey.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder name(String name);

    public abstract Builder columns(ImmutableList<String> columns);

    public abstract DataGeneratorUniqueKey build();
  }
}
