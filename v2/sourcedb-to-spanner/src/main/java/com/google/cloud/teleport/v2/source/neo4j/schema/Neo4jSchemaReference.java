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
package com.google.cloud.teleport.v2.source.neo4j.schema;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;

/** Neo4j database schema reference descriptor. */
@AutoValue
public abstract class Neo4jSchemaReference implements Serializable {

  public abstract @Nullable String databaseName();

  public static Builder builder() {
    return new AutoValue_Neo4jSchemaReference.Builder();
  }

  public String getName() {
    return "Database." + databaseName();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setDatabaseName(@Nullable String value);

    public abstract Neo4jSchemaReference build();
  }
}
