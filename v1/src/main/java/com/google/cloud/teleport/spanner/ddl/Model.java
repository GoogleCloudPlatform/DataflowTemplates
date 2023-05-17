/*
 * Copyright (C) 2021 Google LLC
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import javax.annotation.Nullable;

/** Cloud Spanner model. */
@AutoValue
public abstract class Model implements Serializable {
  private static final long serialVersionUID = 1L;

  @Nullable
  public abstract String name();

  public abstract ImmutableList<ModelColumn> inputColumns();

  public abstract ImmutableList<ModelColumn> outputColumns();

  public abstract boolean remote();

  @Nullable
  public abstract ImmutableList<String> options();

  public abstract Dialect dialect();

  public static Builder builder() {
    return builder(Dialect.GOOGLE_STANDARD_SQL);
  }

  public static Builder builder(Dialect dialect) {
    return new AutoValue_Model.Builder().dialect(dialect).options(ImmutableList.of());
  }

  public abstract Builder toBuilder();

  public void prettyPrint(Appendable appendable) throws IOException {
    if (dialect() != Dialect.GOOGLE_STANDARD_SQL) {
      throw new IllegalArgumentException(String.format("Unrecognized Dialect: %s.", dialect()));
    }

    String identifierQuote = DdlUtilityComponents.identifierQuote(dialect());
    appendable
        .append("CREATE MODEL ")
        .append(identifierQuote)
        .append(name())
        .append(identifierQuote);
    appendable.append("\nINPUT (");
    for (ModelColumn column : inputColumns()) {
      appendable.append("\n\t");
      column.prettyPrint(appendable);
      appendable.append(",");
    }
    appendable.append("\n)");
    appendable.append("\nOUTPUT (");
    for (ModelColumn column : outputColumns()) {
      appendable.append("\n\t");
      column.prettyPrint(appendable);
      appendable.append(",");
    }
    appendable.append("\n)");
    if (remote()) {
      appendable.append("\nREMOTE");
    }
    if (options() != null && !options().isEmpty()) {
      String optionsString = String.join(", ", options());
      appendable.append("\nOPTIONS (").append(optionsString).append(")");
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

  /** A builder for {@link Model}. */
  @AutoValue.Builder
  public abstract static class Builder {
    private Ddl.Builder ddlBuilder;
    private LinkedHashMap<String, ModelColumn> inputColumns = Maps.newLinkedHashMap();
    private LinkedHashMap<String, ModelColumn> outputColumns = Maps.newLinkedHashMap();

    public Builder ddlBuilder(Ddl.Builder ddlBuilder) {
      this.ddlBuilder = ddlBuilder;
      return this;
    }

    public abstract Builder name(String name);

    public abstract String name();

    abstract Builder inputColumns(ImmutableList<ModelColumn> inputColumns);

    abstract Builder outputColumns(ImmutableList<ModelColumn> outputColumns);

    public abstract Builder remote(boolean remote);

    public abstract Builder options(ImmutableList<String> options);

    public abstract Builder dialect(Dialect dialect);

    public abstract Dialect dialect();

    abstract Model autoBuild();

    public Model build() {
      return inputColumns(ImmutableList.copyOf(inputColumns.values()))
          .outputColumns(ImmutableList.copyOf(outputColumns.values()))
          .autoBuild();
    }

    public ModelColumn.Builder inputColumn(String name) {
      ModelColumn column = inputColumns.get(name.toLowerCase());
      if (column != null) {
        return column.toBuilder().modelBuilder(this);
      }
      return ModelColumn.builder(dialect()).name(name).modelBuilder(this);
    }

    public Builder addInputColumn(ModelColumn column) {
      inputColumns.put(column.name().toLowerCase(), column);
      return this;
    }

    public ModelColumn.Builder outputColumn(String name) {
      ModelColumn column = outputColumns.get(name.toLowerCase());
      if (column != null) {
        return column.toBuilder().modelBuilder(this);
      }
      return ModelColumn.builder(dialect()).name(name).modelBuilder(this);
    }

    public Builder addOutputColumn(ModelColumn column) {
      outputColumns.put(column.name().toLowerCase(), column);
      return this;
    }

    public Ddl.Builder endModel() {
      ddlBuilder.addModel(build());
      return ddlBuilder;
    }
  }
}
