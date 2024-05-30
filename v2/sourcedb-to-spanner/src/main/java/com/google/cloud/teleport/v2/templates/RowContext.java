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
package com.google.cloud.teleport.v2.templates;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import javax.annotation.Nullable;

/** Carrier of all the context of a given row through the duration of this pipeline. */
@AutoValue
public abstract class RowContext implements Serializable {

  public abstract SourceRow row();

  @Nullable
  public abstract Mutation mutation();

  @Nullable
  public abstract Throwable err();

  public static Builder builder() {
    return new AutoValue_RowContext.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setRow(SourceRow row);

    public abstract Builder setMutation(Mutation m);

    public abstract Builder setErr(Throwable t);

    public abstract RowContext build();
  }

  public String getStackTraceString() {
    if (err() == null) {
      return null;
    }
    StringWriter sw = new StringWriter();
    err().printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }
}
