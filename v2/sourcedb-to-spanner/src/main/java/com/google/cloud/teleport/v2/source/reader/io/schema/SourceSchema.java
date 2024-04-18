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
package com.google.cloud.teleport.v2.source.reader.io.schema;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;

/**
 * Value class that encloses both the source schema as read from the source database's system tables
 * and the avroSchema for the same for all the tables.
 */
@AutoValue
public abstract class SourceSchema implements Serializable {
  public abstract SourceSchemaReference schemaReference();

  public abstract ImmutableList<SourceTableSchema> tableSchemas();

  public static Builder builder() {
    return new AutoValue_SourceSchema.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSchemaReference(SourceSchemaReference sourceSchemaReference);

    abstract ImmutableList.Builder<SourceTableSchema> tableSchemasBuilder();

    public final Builder addTableSchema(SourceTableSchema tableSchema) {
      this.tableSchemasBuilder().add(tableSchema);
      return this;
    }

    public abstract SourceSchema build();
  }
}
