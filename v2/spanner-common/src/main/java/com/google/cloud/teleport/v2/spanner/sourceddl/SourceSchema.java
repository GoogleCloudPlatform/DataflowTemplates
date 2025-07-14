/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.spanner.sourceddl;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import javax.annotation.Nullable;

/** Represents the schema of a source database. */
@AutoValue
public abstract class SourceSchema implements Serializable {

  private static final long serialVersionUID = 1L;

  public abstract String databaseName();

  public abstract SourceDatabaseType sourceType();

  public abstract ImmutableMap<String, SourceTable> tables();

  @Nullable
  public abstract String version();

  public static Builder builder(SourceDatabaseType sourceType) {
    return new AutoValue_SourceSchema.Builder().sourceType(sourceType).tables(ImmutableMap.of());
  }

  public abstract Builder toBuilder();

  /** A builder for {@link SourceSchema}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder databaseName(String databaseName);

    public abstract Builder sourceType(SourceDatabaseType sourceType);

    public abstract Builder tables(ImmutableMap<String, SourceTable> tables);

    public abstract Builder version(String version);

    public abstract SourceSchema build();
  }

  public SourceTable table(String tableName) {
    return tables().get(tableName);
  }
}
