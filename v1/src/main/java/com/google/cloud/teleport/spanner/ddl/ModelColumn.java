/*
 * Copyright (C) 2018 Google LLC
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

import static com.google.cloud.teleport.spanner.common.DdlUtils.quoteIdentifier;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.common.SizedType;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/** Cloud Spanner model column. */
@AutoValue
public abstract class ModelColumn implements Serializable {
  private static final long serialVersionUID = 1;

  public abstract String name();

  public abstract Type type();

  @Nullable
  public abstract Integer size();

  public abstract ImmutableList<String> columnOptions();

  public abstract Dialect dialect();

  public static Builder builder() {
    return builder(Dialect.GOOGLE_STANDARD_SQL);
  }

  public static Builder builder(Dialect dialect) {
    return new AutoValue_ModelColumn.Builder().dialect(dialect).columnOptions(ImmutableList.of());
  }

  public abstract Builder toBuilder();

  public String typeString() {
    return SizedType.typeString(type(), size());
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    if (dialect() != Dialect.GOOGLE_STANDARD_SQL) {
      throw new IllegalArgumentException(String.format("Unrecognized Dialect: %s.", dialect()));
    }
    appendable
        .append(String.format("%1$-40s", quoteIdentifier(name(), dialect())))
        .append(typeString());

    if (columnOptions() != null && !columnOptions().isEmpty()) {
      String optionsString = columnOptions().stream().collect(Collectors.joining(", "));
      if (!optionsString.isEmpty()) {
        appendable.append(" OPTIONS (").append(optionsString).append(")");
      }
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

  /** A builder for {@link ModelColumn}. */
  @AutoValue.Builder
  public abstract static class Builder {

    private Model.Builder modelBuilder;

    Builder modelBuilder(Model.Builder modelBuilder) {
      this.modelBuilder = modelBuilder;
      return this;
    }

    abstract Builder name(String name);

    public abstract Builder type(Type type);

    public abstract Builder size(Integer size);

    public abstract Builder columnOptions(ImmutableList<String> options);

    abstract Builder dialect(Dialect dialect);

    abstract Dialect dialect();

    public abstract ModelColumn autoBuild();

    public Builder parseType(String spannerType) {
      SizedType sizedType = SizedType.parseSpannerType(spannerType, dialect());
      return type(sizedType.type).size(sizedType.size);
    }

    public Model.Builder endInputColumn() {
      modelBuilder.addInputColumn(this.autoBuild());
      return modelBuilder;
    }

    public Model.Builder endOutputColumn() {
      modelBuilder.addOutputColumn(this.autoBuild());
      return modelBuilder;
    }
  }
}
