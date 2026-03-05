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
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.Map;

/** Represents the entire schema for data generation. */
@AutoValue
public abstract class DataGeneratorSchema implements Serializable {

  /** Map of table name to table definition. */
  public abstract ImmutableMap<String, DataGeneratorTable> tables();

  /** The dialect of the sink database. */
  public abstract SinkDialect dialect();

  public static Builder builder() {
    return new AutoValue_DataGeneratorSchema.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder tables(Map<String, DataGeneratorTable> tables);

    public abstract Builder tables(ImmutableMap<String, DataGeneratorTable> tables);

    public abstract Builder dialect(SinkDialect dialect);

    public abstract DataGeneratorSchema build();
  }
}
