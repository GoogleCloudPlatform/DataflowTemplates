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

/**
 * Value Class to Provide reference to a sourceTable as it completes reading.
 *
 * <p><b>Note:</b> The generation of completion needs at least once `reduce` operation. So the most
 * simple, count of rows read, is also provided. This can be enhanced with more (parallel) combiners
 * like min or max or distribution of readTime, as and when needed.
 */
@AutoValue
public abstract class SourceTableReference implements Serializable {
  public abstract SourceSchemaReference sourceSchemaReference();

  public abstract String sourceTableName();

  public abstract String sourceTableSchemaUUID();

  public abstract Long recordCount();

  public static Builder builder() {
    Builder builder = new AutoValue_SourceTableReference.Builder();
    builder.setRecordCount(0);
    return builder;
  }

  /**
   * Returns a stable unique name to be used in PTransforms.
   *
   * @return name of the {@link SourceTableReference}
   */
  public String getName() {
    return (new StringBuilder())
        .append(this.sourceSchemaReference().getName())
        .append(".Table.")
        .append(this.sourceTableName())
        .toString();
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSourceSchemaReference(SourceSchemaReference value);

    public abstract Builder setSourceTableName(String value);

    public abstract Builder setSourceTableSchemaUUID(String value);

    public abstract Builder setRecordCount(long value);

    public abstract SourceTableReference build();
  }
}
