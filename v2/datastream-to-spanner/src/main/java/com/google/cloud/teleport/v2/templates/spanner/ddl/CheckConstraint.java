/*
 * Copyright (C) 2020 Google LLC
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
package com.google.cloud.teleport.v2.templates.spanner.ddl;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;

/** Cloud Spanner CHECK CONSTRAINT. */
@AutoValue
public abstract class CheckConstraint implements Serializable {
  private static final long serialVersionUID = 286089906L;

  public abstract String name();

  public abstract String expression();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_CheckConstraint.Builder();
  }

  private void prettyPrint(Appendable appendable) throws IOException {
    appendable
        .append("CONSTRAINT `")
        .append(name())
        .append("` CHECK (")
        .append(expression())
        .append(")");
  }

  public String prettyPrint() {
    StringBuilder sb = new StringBuilder();
    try {
      prettyPrint(sb);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return prettyPrint();
  }

  /** A builder for {@link CheckConstraint}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder name(String name);

    public abstract Builder expression(String expr);

    public abstract CheckConstraint build();
  }
}
