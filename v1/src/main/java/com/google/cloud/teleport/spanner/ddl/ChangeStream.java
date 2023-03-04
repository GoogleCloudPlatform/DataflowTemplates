/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.spanner.ddl;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Dialect;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;

/** Cloud Spanner change stream. */
@AutoValue
public abstract class ChangeStream implements Serializable {

  private static final long serialVersionUID = 1L;

  public abstract String name();

  @Nullable
  public abstract String forClause();

  @Nullable
  public abstract ImmutableList<String> options();

  public abstract Dialect dialect();

  public abstract Builder toBuilder();

  public static Builder builder(Dialect dialect) {
    return new AutoValue_ChangeStream.Builder().dialect(dialect);
  }

  public static Builder builder() {
    return builder(Dialect.GOOGLE_STANDARD_SQL);
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    if (dialect() != Dialect.GOOGLE_STANDARD_SQL && dialect() != Dialect.POSTGRESQL) {
      throw new IllegalArgumentException(String.format("Unrecognized Dialect: %s.", dialect()));
    }
    String identifierQuote = DdlUtilityComponents.identifierQuote(dialect());
    appendable
        .append("CREATE CHANGE STREAM ")
        .append(identifierQuote)
        .append(name())
        .append(identifierQuote);
    if (!Strings.isNullOrEmpty(forClause())) {
      appendable.append("\n\t").append(forClause());
    }
    if (options() != null && !options().isEmpty()) {
      String optionsString = String.join(", ", options());
      appendable.append("\n\t");
      if (dialect() == Dialect.GOOGLE_STANDARD_SQL) {
        appendable.append("OPTIONS ");
      } else if (dialect() == Dialect.POSTGRESQL) {
        appendable.append("WITH ");
      }
      appendable.append("(").append(optionsString).append(")");
    }
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

  /** A builder for {@link ChangeStream}. */
  @AutoValue.Builder
  public abstract static class Builder {
    private Ddl.Builder ddlBuilder;

    public Builder ddlBuilder(Ddl.Builder ddlBuilder) {
      this.ddlBuilder = ddlBuilder;
      return this;
    }

    public abstract Builder name(String name);

    public abstract Builder forClause(String name);

    public abstract Builder options(ImmutableList<String> options);

    abstract Builder dialect(Dialect dialect);

    public abstract ChangeStream build();

    public Ddl.Builder endChangeStream() {
      ddlBuilder.addChangeStream(build());
      return ddlBuilder;
    }
  }
}
