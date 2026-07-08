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
package com.google.cloud.teleport.v2.spanner.sourceddl;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;

/** Represents an index in a source database. */
@AutoValue
public abstract class SourceIndex implements Serializable {

  public abstract String name();

  public abstract ImmutableList<String> columns();

  public abstract boolean isUnique();

  public abstract boolean isPrimary();

  public abstract String tableName();

  public static Builder builder() {
    return new AutoValue_SourceIndex.Builder()
        .columns(ImmutableList.of())
        .isUnique(false)
        .isPrimary(false);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder name(String name);

    public abstract Builder columns(ImmutableList<String> columns);

    public abstract Builder isUnique(boolean isUnique);

    public abstract Builder isPrimary(boolean isPrimary);

    public abstract Builder tableName(String tableName);

    public abstract SourceIndex build();
  }
}
