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
import java.io.Serializable;
import javax.annotation.Nullable;

/** Value class to enclose the database name and PG namespace. */
@AutoValue
public abstract class SourceSchemaReference implements Serializable {

  public abstract String dbName();

  @Nullable
  public abstract String namespace();

  public static Builder builder() {
    return new AutoValue_SourceSchemaReference.Builder().setNamespace(null);
  }

  /**
   * Returns a stable unique name to be used in PTransforms.
   *
   * @return name of the {@link SourceTableReference}
   */
  public String getName() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("Db.").append(this.dbName());
    if (this.namespace() != null) {
      stringBuilder.append(".Namespace.").append(this.namespace());
    }
    return stringBuilder.toString();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setDbName(String value);

    public abstract Builder setNamespace(String value);

    public abstract SourceSchemaReference build();
  }
}
